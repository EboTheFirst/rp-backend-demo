"""
Simple in-memory cache for expensive operations.
"""
from functools import wraps
from datetime import datetime, timedelta

# Simple in-memory cache
_cache = {}

def timed_cache(seconds=300):
    """
    Decorator that caches a function's return value for a specified time.
    
    Args:
        seconds: Number of seconds to cache the result
        
    Returns:
        Decorated function with caching
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Create a cache key from function name and arguments
            key = str(func.__name__) + str(args) + str(kwargs)
            
            # Check if result is in cache and not expired
            if key in _cache:
                result, timestamp = _cache[key]
                if datetime.now() - timestamp < timedelta(seconds=seconds):
                    return result
            
            # Call the function and cache the result
            result = func(*args, **kwargs)
            _cache[key] = (result, datetime.now())
            return result
        return wrapper
    return decorator

def clear_cache():
    """Clear the entire cache."""
    global _cache
    _cache = {}