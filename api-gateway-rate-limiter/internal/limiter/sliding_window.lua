-- Sliding Window Counter Rate Limiting Script (RFC / Production Ready)
-- KEYS[1]: Base Redis key (with hash tag to ensure Redis Cluster slot affinity, e.g. "rl:{ip}")
-- ARGV[1]: Limit (maximum allowed requests)
-- ARGV[2]: Window duration in milliseconds
-- ARGV[3]: Current timestamp in milliseconds
-- ARGV[4]: Request cost (default: 1)

local base_key = KEYS[1]
local limit = tonumber(ARGV[1])
local window_ms = tonumber(ARGV[2])
local now_ms = tonumber(ARGV[3])
local cost = tonumber(ARGV[4]) or 1

-- 1. Determine current and previous window identifiers
local curr_window_id = math.floor(now_ms / window_ms)
local prev_window_id = curr_window_id - 1

local curr_key = base_key .. ":" .. curr_window_id
local prev_key = base_key .. ":" .. prev_window_id

-- 2. Fetch counts in a single atomic script execution
local curr_count = tonumber(redis.call("GET", curr_key) or "0")
local prev_count = tonumber(redis.call("GET", prev_key) or "0")

-- 3. Calculate sliding window weight
local time_in_curr = now_ms % window_ms
local elapsed_ratio = time_in_curr / window_ms
local weight_prev = 1.0 - elapsed_ratio

-- Formula: Estimated = Current + (Previous * (1 - ElapsedRatio))
local estimated_requests = curr_count + (prev_count * weight_prev)

-- 4. Check if request is within capacity
if (estimated_requests + cost) <= limit then
    -- Allowed: increment current window counter
    redis.call("INCRBY", curr_key, cost)

    -- Expire current key after 2 full windows so it survives into next window as prev_key
    local ttl_sec = math.ceil((window_ms * 2) / 1000) + 1
    redis.call("EXPIRE", curr_key, ttl_sec)

    local remaining = math.max(0, math.floor(limit - estimated_requests - cost))
    local reset_ms = (curr_window_id + 1) * window_ms

    -- Returns: {allowed (1), remaining, reset_ms, retry_after_ms (0)}
    return {1, remaining, reset_ms, 0}
else
    -- Denied
    local reset_ms = (curr_window_id + 1) * window_ms
    local retry_after_ms = window_ms - time_in_curr
    if retry_after_ms < 1 then
        retry_after_ms = 1
    end

    -- Returns: {allowed (0), remaining (0), reset_ms, retry_after_ms}
    return {0, 0, reset_ms, retry_after_ms}
end
