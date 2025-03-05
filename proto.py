import time
import redis
import threading
import logging
import random
from typing import Dict, List, Optional, Set, Tuple
from collections import deque
from statistics import mean, median
import uuid

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("distributed_rate_limiter")

class RegionRateLimiter:
    """
    Region-based rate limiter that handles borrowing directly during rate limit checks.
    Each region has its own Redis instance and can borrow capacity from other regions.
    """
    
    def __init__(self, 
                 region_name: str,
                 redis_host: str,
                 redis_port: int,
                 base_limit: int,
                 global_limit: int,
                 peer_regions: Dict[str, Dict[str, str]],
                 window_size_seconds: int = 1,
                 borrow_threshold: float = 0.8,
                 borrow_cooldown_seconds: int = 5):
        """
        Initialize the regional rate limiter
        
        Args:
            region_name: Name of this region
            redis_host: Hostname for this region's Redis
            redis_port: Port for this region's Redis
            base_limit: Base rate limit for this region
            global_limit: Total global rate limit
            peer_regions: Dictionary of peer region information {name: {host, port}}
            window_size_seconds: Size of the sliding window for rate limiting
            borrow_threshold: Utilization threshold that triggers borrowing
            borrow_cooldown_seconds: Minimum time between borrow attempts
        """
        self.region = region_name
        self.base_limit = base_limit
        self.global_limit = global_limit
        self.peer_regions = peer_regions
        self.window_size = window_size_seconds
        self.borrow_threshold = borrow_threshold
        self.borrow_cooldown = borrow_cooldown_seconds
        
        # Connect to this region's Redis
        self.redis = redis.Redis(
            host=redis_host,
            port=redis_port,
            decode_responses=True
        )
        logger.info(f"Connected to region {self.region}'s Redis at {redis_host}:{redis_port}")
        
        # Cache for peer Redis connections
        self._peer_redis: Dict[str, redis.Redis] = {}
        
        # Redis keys
        self.counter_key = f"rate_limit:{self.region}:counter"
        self.limit_key = f"rate_limit:{self.region}:limit"
        self.borrowed_key = f"rate_limit:{self.region}:borrowed"
        self.lent_out_key = f"rate_limit:{self.region}:lent_out"
        self.timestamp_key = f"rate_limit:{self.region}:last_reset"
        self.last_borrow_key = f"rate_limit:{self.region}:last_borrow"
        
        # Initialize Redis with keys
        with self.redis.pipeline() as pipe:
            # Set initial limit
            pipe.set(self.limit_key, base_limit)
            # Reset counter
            pipe.set(self.counter_key, 0)
            # Initialize borrowed amount
            pipe.set(self.borrowed_key, 0)
            # Initialize lent out amount
            pipe.set(self.lent_out_key, 0)
            # Set timestamp
            pipe.set(self.timestamp_key, int(time.time()))
            # Set last borrow time
            pipe.set(self.last_borrow_key, 0)
            # Execute all commands atomically
            pipe.execute()
        
        # Set expiration on the counter key
        self.redis.expire(self.counter_key, self.window_size * 2)
        
        # Register Lua scripts
        self._register_lua_scripts()
        
        # Start background thread to periodically return borrowed capacity
        self.running = True
        #self.return_thread = threading.Thread(target=self._periodic_return_capacity)
        #self.return_thread.daemon = True
        #self.return_thread.start()
        
        # Track stats
        self.borrow_attempts = 0
        self.successful_borrows = 0
        self.cross_region_requests = 0
        
        # Track timing metrics
        self.request_durations = deque(maxlen=1000)  # Store last 1000 request durations
        self.borrow_durations = deque(maxlen=100)    # Store last 100 borrow durations
        self.fast_path_durations = deque(maxlen=1000)  # Store durations when no borrowing needed
        
        logger.info(f"Region rate limiter started for {self.region} with base limit: {base_limit}")
    
    def _register_lua_scripts(self):
        """Register Lua scripts for atomic operations"""
        # Script for check-and-increment with rate limiting
        check_script = """
        local counter_key = KEYS[1]
        local limit_key = KEYS[2]
        local timestamp_key = KEYS[3]
        local window_size = tonumber(ARGV[1])
        local cost = tonumber(ARGV[2])
        
        -- Check if we need to reset the window based on timestamp
        local current_time = redis.call('time')[1]
        local last_reset = tonumber(redis.call('get', timestamp_key) or 0)
        
        if current_time - last_reset >= window_size then
            -- Reset counter and update timestamp if window expired
            redis.call('set', counter_key, 0)
            redis.call('set', timestamp_key, current_time)
        end
        
        -- Get current limit and count
        local limit = tonumber(redis.call('get', limit_key) or 0)
        local current = tonumber(redis.call('get', counter_key) or 0)
        
        -- Calculate utilization for return value
        local utilization = 0
        if limit > 0 then
            utilization = current / limit
        else
            utilization = 1
        end
        
        -- Check if we would exceed limit
        if current + cost <= limit then
            -- Increment the counter and return utilization
            local new_count = redis.call('incrby', counter_key, cost)
            -- Refresh expiration
            redis.call('expire', counter_key, window_size * 2)
            return string.format("%d:%.3f", 1, utilization)  -- 1 means allowed
        else
            -- Return 0 to indicate rejection, along with utilization
            return string.format("%d:%.3f", 0, utilization)  -- 0 means rejected
        end
        """
        self._check_script = self.redis.register_script(check_script)
        
        # Script for borrowing capacity
        borrow_script = """
        local limit_key = KEYS[1]
        local borrowed_key = KEYS[2]
        local amount = tonumber(ARGV[1])
        
        -- Get current limit and borrowed amount
        local current_limit = tonumber(redis.call('get', limit_key) or 0)
        local current_borrowed = tonumber(redis.call('get', borrowed_key) or 0)
        
        -- Increase limit and borrowed amount
        redis.call('incrby', limit_key, amount)
        redis.call('incrby', borrowed_key, amount)
        
        return current_limit + amount
        """
        self._borrow_script = self.redis.register_script(borrow_script)
        
        # Script for lending capacity
        lend_script = """
        local limit_key = KEYS[1]
        local lent_out_key = KEYS[2]
        local amount = tonumber(ARGV[1])
        
        -- Get current limit and lent out amount
        local current_limit = tonumber(redis.call('get', limit_key) or 0)
        local current_lent = tonumber(redis.call('get', lent_out_key) or 0)
        
        -- Check if we have enough capacity to lend
        if current_limit - amount < 0 then
            return 0  -- Cannot lend
        end
        
        -- Decrease limit and increase lent out amount
        redis.call('decrby', limit_key, amount)
        redis.call('incrby', lent_out_key, amount)
        
        return amount
        """
        self._lend_script = self.redis.register_script(lend_script)
        
        # Script for returning borrowed capacity
        return_script = """
        local limit_key = KEYS[1]
        local borrowed_key = KEYS[2]
        local amount = tonumber(ARGV[1])
        
        -- Get current borrowed amount
        local current_borrowed = tonumber(redis.call('get', borrowed_key) or 0)
        
        -- Don't return more than what was borrowed
        local to_return = math.min(amount, current_borrowed)
        if to_return <= 0 then
            return 0
        end
        
        -- Decrease limit and borrowed amount
        redis.call('decrby', limit_key, to_return)
        redis.call('decrby', borrowed_key, to_return)
        
        return to_return
        """
        self._return_script = self.redis.register_script(return_script)
        
        # Script for reclaiming lent capacity
        reclaim_script = """
        local limit_key = KEYS[1]
        local lent_out_key = KEYS[2]
        local amount = tonumber(ARGV[1])
        
        -- Get current lent out amount
        local current_lent = tonumber(redis.call('get', lent_out_key) or 0)
        
        -- Don't reclaim more than what was lent out
        local to_reclaim = math.min(amount, current_lent)
        if to_reclaim <= 0 then
            return 0
        end
        
        -- Increase limit and decrease lent out amount
        redis.call('incrby', limit_key, to_reclaim)
        redis.call('decrby', lent_out_key, to_reclaim)
        
        return to_reclaim
        """
        self._reclaim_script = self.redis.register_script(reclaim_script)
    
    def _get_peer_redis(self, region_name: str) -> Optional[redis.Redis]:
        """Get or create a Redis connection to a peer region"""
        if region_name not in self._peer_redis:
            if region_name not in self.peer_regions:
                logger.error(f"Unknown peer region: {region_name}")
                return None
            
            peer_info = self.peer_regions[region_name]
            try:
                self._peer_redis[region_name] = redis.Redis(
                    host=peer_info['host'],
                    port=peer_info['port'],
                    decode_responses=True
                )
            except Exception as e:
                logger.error(f"Failed to connect to peer {region_name}: {e}")
                return None
        
        return self._peer_redis[region_name]
    
    def _periodic_return_capacity(self):
        """Periodically return borrowed capacity"""
        while self.running:
            try:
                # Sleep for a while
                time.sleep(self.window_size / 4)  # Return capacity every quarter window
                
                # Get current borrowed amount
                borrowed = int(self.redis.get(self.borrowed_key) or 0)
                if borrowed <= 0:
                    continue
                
                # Get current utilization
                current = int(self.redis.get(self.counter_key) or 0)
                limit = int(self.redis.get(self.limit_key) or 0)
                
                if limit == 0:
                    utilization = 1.0
                else:
                    utilization = current / limit
                
                # If utilization is low enough, return some capacity
                if utilization < 0.5:
                    # Return up to 25% of borrowed capacity
                    amount_to_return = max(1, int(borrowed * 0.25))
                    returned = int(self._return_script(keys=[self.limit_key, self.borrowed_key], args=[amount_to_return]))
                    
                    if returned > 0:
                        logger.info(f"{self.region} returned {returned} capacity. Utilization: {utilization:.2f}")
            except Exception as e:
                logger.error(f"Error in periodic return for {self.region}: {e}")
    
    def _borrow_from_peers(self, needed_amount: int) -> Tuple[int, float]:
        """
        Attempt to borrow capacity from peer regions
        
        Returns:
            Tuple of (amount_borrowed, duration_in_ms)
        """
        start_time = time.time()
        
        # Get last borrow time
        last_borrow_time = int(self.redis.get(self.last_borrow_key) or 0)
        current_time = int(time.time())
        
        # Check if we're in cooldown period
        if current_time - last_borrow_time < self.borrow_cooldown:
            logger.debug(f"{self.region} in borrow cooldown. Skipping.")
            return 0, 0.0
        
        # Update last borrow time
        self.redis.set(self.last_borrow_key, current_time)
        
        # Track for stats
        self.borrow_attempts += 1
        
        # Shuffle peer regions to avoid always hitting the same ones first
        peer_names = list(self.peer_regions.keys())
        random.shuffle(peer_names)
        
        total_borrowed = 0
        for peer_name in peer_names:
            # stop once we borrowed enough
            if total_borrowed >= needed_amount:
                break
                
            peer_redis = self._get_peer_redis(peer_name)
            if not peer_redis:
                continue
            
            try:
                # Request some capacity from this peer
                # We'll request a bit more than we need to account for failures
                request_amount = needed_amount - total_borrowed
                
                # Try to lend from peer
                peer_lent_out_key = f"rate_limit:{peer_name}:lent_out"
                peer_limit_key = f"rate_limit:{peer_name}:limit"
                
                # Get peer utilization
                peer_counter_key = f"rate_limit:{peer_name}:counter"
                peer_usage = int(peer_redis.get(peer_counter_key) or 0)
                peer_limit = int(peer_redis.get(peer_limit_key) or 0)
                
                if peer_limit == 0:
                    peer_utilization = 1.0
                else:
                    peer_utilization = peer_usage / peer_limit
                
                # Only borrow from peers with low utilization
                if peer_utilization >= 0.6:
                    logger.debug(f"Peer {peer_name} utilization too high: {peer_utilization:.2f}")
                    continue
                
                # Calculate how much to borrow - the less utilized, the more we can borrow
                max_borrow_pct = 0.3 * (1 - peer_utilization)  # Up to 30% of their capacity if they're at 0% utilization
                max_borrow = int(peer_limit * max_borrow_pct)
                amount_to_borrow = min(request_amount, max_borrow)
                
                if amount_to_borrow <= 0:
                    continue
                
                # Execute the lend script on the peer
                # First we need to register the script with the peer Redis connection
                peer_lend_script = peer_redis.register_script("""
                local limit_key = KEYS[1]
                local lent_out_key = KEYS[2]
                local amount = tonumber(ARGV[1])
                
                -- Get current limit and lent out amount
                local current_limit = tonumber(redis.call('get', limit_key) or 0)
                local current_lent = tonumber(redis.call('get', lent_out_key) or 0)
                
                -- Check if we have enough capacity to lend
                if current_limit - amount < 0 then
                    return 0  -- Cannot lend
                end
                
                -- Decrease limit and increase lent out amount
                redis.call('decrby', limit_key, amount)
                redis.call('incrby', lent_out_key, amount)
                
                return amount
                """)
                
                borrowed = int(peer_lend_script(keys=[peer_limit_key, peer_lent_out_key], args=[amount_to_borrow]))
                
                # Count this as cross-region communication
                self.cross_region_requests += 1
                
                if borrowed <= 0:
                    logger.debug(f"Peer {peer_name} unable to lend capacity")
                    continue
                
                # Add the borrowed capacity to our limit
                self._borrow_script(keys=[self.limit_key, self.borrowed_key], args=[borrowed])
                
                total_borrowed += borrowed
                logger.info(f"{self.region} borrowed {borrowed} from {peer_name} (utilization: {peer_utilization:.2f})")
            except Exception as e:
                logger.error(f"Error borrowing from {peer_name}: {e}")
        
        if total_borrowed > 0:
            self.successful_borrows += 1
        
        # Calculate duration in milliseconds
        duration_ms = (time.time() - start_time) * 1000
        self.borrow_durations.append(duration_ms)
            
        return total_borrowed, duration_ms
    
    def is_allowed(self, key: str = "", cost: int = 1) -> Tuple[bool, float]:
        """
        Check if a request is allowed under the rate limit
        
        Args:
            key: Optional key for specific rate limiting (unused in this implementation)
            cost: Cost of this request
            
        Returns:
            Tuple of (is_allowed, duration_in_ms)
        """
        # Start timing the request
        start_time = time.time()
        borrow_time = 0.0
        
        # Execute the check script
        result = self._check_script(
            keys=[self.counter_key, self.limit_key, self.timestamp_key], 
            args=[self.window_size, cost]
        )
        
        # Parse result: "status:utilization"
        parts = result.split(":")
        is_allowed = int(parts[0]) == 1
        utilization = float(parts[1])
        
        # If request is denied, try to borrow capacity
        if not is_allowed:
            # Only borrow if we're at high utilization
            if utilization >= 0.9:
                # Calculate how much to borrow - enough to handle this request plus some headroom
                headroom = max(10, int(self.base_limit * 0.1))  # At least 10 requests or 10% of base limit
                needed_amount = cost + headroom
                
                # Try to borrow from peers
                borrowed, borrow_time = self._borrow_from_peers(needed_amount)
                
                if borrowed > 0:
                    # Re-check if request is allowed with new capacity
                    # Note: This is a recursive call that will add to the total duration
                    allowed_result, recursive_duration = self.is_allowed(key, cost)
                    
                    # Calculate total duration including both attempts
                    total_duration = (time.time() - start_time) * 1000
                    self.request_durations.append(total_duration)
                    
                    return allowed_result, total_duration
        elif utilization >= self.borrow_threshold:
            # If we're allowed but close to capacity, try to borrow proactively
            # Do this in a separate thread to not block the request
            def borrow_proactively():
                headroom = max(10, int(self.base_limit * 0.2))  # 20% of base limit for proactive borrowing
                self._borrow_from_peers(headroom)
            
            borrow_thread = threading.Thread(target=borrow_proactively)
            borrow_thread.daemon = True
            borrow_thread.start()
        
        # Calculate the duration in milliseconds
        duration_ms = (time.time() - start_time) * 1000
        
        # Record the duration in the appropriate collection
        self.request_durations.append(duration_ms)
        if borrow_time == 0.0:
            self.fast_path_durations.append(duration_ms)
        
        return is_allowed, duration_ms
    
    def get_stats(self) -> Dict:
        """Get current stats for this region"""
        with self.redis.pipeline() as pipe:
            pipe.get(self.counter_key)
            pipe.get(self.limit_key)
            pipe.get(self.borrowed_key)
            pipe.get(self.lent_out_key)
            counter, limit, borrowed, lent_out = pipe.execute()
        
        counter = int(counter or 0)
        limit = int(limit or 0)
        borrowed = int(borrowed or 0)
        lent_out = int(lent_out or 0)
        
        if limit == 0:
            utilization = 1.0
        else:
            utilization = counter / limit
        
        # Calculate timing statistics
        timing_stats = {
            "request_count": len(self.request_durations),
            "request_avg_ms": round(mean(self.request_durations) if self.request_durations else 0, 2),
            "request_median_ms": round(median(self.request_durations) if self.request_durations else 0, 2),
            "request_max_ms": round(max(self.request_durations) if self.request_durations else 0, 2),
            "request_min_ms": round(min(self.request_durations) if self.request_durations else 0, 2),
            
            "fast_path_avg_ms": round(mean(self.fast_path_durations) if self.fast_path_durations else 0, 2),
            "fast_path_median_ms": round(median(self.fast_path_durations) if self.fast_path_durations else 0, 2),
            
            "borrow_count": len(self.borrow_durations),
            "borrow_avg_ms": round(mean(self.borrow_durations) if self.borrow_durations else 0, 2),
            "borrow_median_ms": round(median(self.borrow_durations) if self.borrow_durations else 0, 2),
            "borrow_max_ms": round(max(self.borrow_durations) if self.borrow_durations else 0, 2)
        }
            
        return {
            "region": self.region,
            "usage": counter,
            "current_limit": limit,
            "base_limit": self.base_limit,
            "borrowed": borrowed,
            "lent_out": lent_out,
            "utilization": utilization,
            "borrow_attempts": self.borrow_attempts,
            "successful_borrows": self.successful_borrows,
            "cross_region_requests": self.cross_region_requests,
            "timing": timing_stats
        }
    
    def shutdown(self):
        """Shutdown the rate limiter"""
        self.running = False
        #self.return_thread.join(timeout=1.0)
        logger.info(f"Region rate limiter for {self.region} shut down")


# Example usage and simulation
def simulate_traffic():
    """Simulate traffic across multiple regions with direct borrowing"""
    
    # Define global limit
    GLOBAL_LIMIT = 1000  # Global limit of 1000 requests per second
    
    # Define regions
    regions_config = {
        "us-east": {
            "host": "localhost",
            "port": 7371,
            "base_limit": 400  # 40% of global limit
        },
        "us-west": {
            "host": "localhost",
            "port": 7372,
            "base_limit": 300  # 30% of global limit
        },
        "eu-west": {
            "host": "localhost",
            "port": 7373,
            "base_limit": 200  # 20% of global limit
        },
        "ap-east": {
            "host": "localhost",
            "port": 7374,
            "base_limit": 100  # 10% of global limit
        }
    }
    
    # Create peer configurations
    peer_configs = {}
    for region_name, config in regions_config.items():
        # For each region, create a dict of all other regions
        peers = {}
        for peer_name, peer_config in regions_config.items():
            if peer_name != region_name:
                peers[peer_name] = {
                    "host": peer_config["host"],
                    "port": peer_config["port"]
                }
        peer_configs[region_name] = peers
    
    # Create rate limiters for each region
    limiters = {}
    for region_name, config in regions_config.items():
        limiters[region_name] = RegionRateLimiter(
            region_name=region_name,
            redis_host=config["host"],
            redis_port=config["port"],
            base_limit=config["base_limit"],
            global_limit=GLOBAL_LIMIT,
            peer_regions=peer_configs[region_name],
            window_size_seconds=1,
            borrow_threshold=0.8,
            borrow_cooldown_seconds=5
        )
    
    # Traffic patterns with quiet periods
    # Each region has its own pattern with different quiet periods

    # Base traffic patterns
    base_traffic = {
        "us-east": lambda: random.randint(400, 500),  # 30-50 requests per second
        #"us-west": lambda: random.randint(200, 300),  # 20-30 requests per second
        "us-west": lambda: random.randint(1, 2),  # 20-30 requests per second
        "eu-west": lambda: random.randint(50, 150),   # 5-15 requests per second
        "ap-east": lambda: random.randint(10, 50)     # 1-5 requests per second
    }
    
    # Schedule quiet periods for each region
    # Format: (start_time, duration_in_seconds)
    quiet_periods = {
        "us-east": [(20, 15), (65, 10)],       # Quiet after 20s for 15s, and after 65s for 10s
        "us-west": [(35, 20), (90, 15)],       # Quiet after 35s for 20s, and after 90s for 15s
        "eu-west": [(15, 10), (50, 10), (100, 10)],  # Three short quiet periods
        "ap-east": [(40, 40)]                  # One long quiet period in the middle
    }
    
    # Function to determine if a region is in a quiet period
    def is_quiet_period(region, current_time):
        elapsed = current_time - start_time
        for start, duration in quiet_periods[region]:
            if start <= elapsed < (start + duration):
                return True
        return False
    
    # Track statistics
    stats = {
        "allowed": 0,
        "rejected": 0,
        "by_region": {region: {"allowed": 0, "rejected": 0} for region in limiters}
    }
    
    # Run simulation for 2 minutes
    start_time = time.time()
    end_time = start_time + 180  # 3 minutes
    
    # Add a spike to eu-west after 1 minute
    spike_time = start_time + 60
    spike_active = False
    
    try:
        while time.time() < end_time:
            current_time = time.time()
            
            # Check if we should activate the spike
            if not spike_active and current_time >= spike_time:
                logger.info("*** TRAFFIC SPIKE STARTED IN EU-WEST ***")
                original_eu_west_pattern = base_traffic["eu-west"]
                base_traffic["eu-west"] = lambda: random.randint(200, 250)  # Sudden surge
                spike_active = True
            
            # Process requests for each region
            for region_name, limiter in limiters.items():
                # Generate requests based on traffic pattern
                request_count = base_traffic[region_name]()
                
                # Process each request
                for _ in range(request_count):
                    request_id = str(uuid.uuid4())
                    allowed, duration = limiter.is_allowed(key=request_id)
                    
                    if allowed:
                        stats["allowed"] += 1
                        stats["by_region"][region_name]["allowed"] += 1
                    else:
                        stats["rejected"] += 1
                        stats["by_region"][region_name]["rejected"] += 1
            
            # Output current statistics every 5 seconds
            if int(current_time) % 5 == 0 and int(current_time) != int(current_time - 0.1):  # Avoid duplicate logs
                total_requests = stats["allowed"] + stats["rejected"]
                if total_requests > 0:
                    allowed_pct = (stats["allowed"] / total_requests) * 100
                else:
                    allowed_pct = 0
                    
                logger.info(f"Stats: {stats['allowed']} allowed ({allowed_pct:.1f}%), {stats['rejected']} rejected")
                
                logger.info("Current state:")
                for region, limiter in limiters.items():
                    region_stats = limiter.get_stats()
                    quiet_status = "QUIET" if is_quiet_period(region, current_time) else "ACTIVE"
                    logger.info(f"  {region} [{quiet_status}]: {region_stats['usage']}/{region_stats['current_limit']} " +
                              f"({region_stats['utilization']:.2f}), borrowed: {region_stats['borrowed']}, " +
                              f"lent: {region_stats['lent_out']}")
            
            # Sleep a short time to control simulation speed
            time.sleep(0.9)
            
    except KeyboardInterrupt:
        logger.info("Simulation stopped by user")
    finally:
        # Reset the traffic pattern for consistency in future runs
        if spike_active:
            base_traffic["eu-west"] = original_eu_west_pattern
            
        # Output final statistics
        logger.info("=== FINAL STATISTICS ===")
        total_requests = stats['allowed'] + stats['rejected']
        logger.info(f"Total requests: {total_requests}")
        
        if total_requests > 0:
            allowed_pct = (stats['allowed'] / total_requests) * 100
            logger.info(f"Allowed: {stats['allowed']} ({allowed_pct:.1f}%)")
        else:
            logger.info(f"Allowed: {stats['allowed']} (0.0%)")
            
        logger.info(f"Rejected: {stats['rejected']}")
        
        # Region-specific stats
        logger.info("By region:")
        for region, limiter in limiters.items():
            region_stats = limiter.get_stats()
            region_allowed = stats["by_region"][region]["allowed"]
            region_rejected = stats["by_region"][region]["rejected"]
            region_total = region_allowed + region_rejected
            
            if region_total > 0:
                region_allowed_pct = (region_allowed / region_total) * 100
            else:
                region_allowed_pct = 0
                
            logger.info(f"  {region}:")
            logger.info(f"    Requests: {region_allowed} allowed ({region_allowed_pct:.1f}%), {region_rejected} rejected")
            logger.info(f"    Final limit: {region_stats['current_limit']} (base: {region_stats['base_limit']})")
            logger.info(f"    Borrowed: {region_stats['borrowed']}, Lent out: {region_stats['lent_out']}")
            logger.info(f"    Borrow attempts: {region_stats['borrow_attempts']}, Successful: {region_stats['successful_borrows']}")
            logger.info(f"    Cross-region requests: {region_stats['cross_region_requests']}")
            
            # Log timing metrics
            timing = region_stats['timing']
            logger.info(f"    Timing metrics:")
            logger.info(f"      Average request: {timing['request_avg_ms']}ms (median: {timing['request_median_ms']}ms)")
            logger.info(f"      Fast path: {timing['fast_path_avg_ms']}ms (median: {timing['fast_path_median_ms']}ms)")
            logger.info(f"      Borrowing: {timing['borrow_avg_ms']}ms (median: {timing['borrow_median_ms']}ms)")
            logger.info(f"      Max request: {timing['request_max_ms']}ms, Max borrow: {timing['borrow_max_ms']}ms")
        
        # Calculate global drift
        total_capacity = sum(limiter.get_stats()['current_limit'] for limiter in limiters.values())
        capacity_drift = total_capacity - GLOBAL_LIMIT
        logger.info(f"Global limit drift: {capacity_drift} ({(capacity_drift / GLOBAL_LIMIT) * 100:.1f}%)")
        
        # Shutdown all limiters
        for limiter in limiters.values():
            limiter.shutdown()
        
if __name__ == "__main__":
    simulate_traffic()
