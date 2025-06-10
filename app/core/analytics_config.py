"""
Configuration settings for analytics functions.
These can be adjusted based on business requirements.
"""

# Segmentation thresholds
CUSTOMER_SEGMENTATION = {
    "high_threshold": 800,  # Customers with total spend > 800 are high value
    "mid_threshold": 500,   # Customers with total spend > 500 and <= 800 are mid value
}

MERCHANT_SEGMENTATION = {
    "high_threshold": 10000,  # Merchants with total sales > 10000 are high value
    "mid_threshold": 5000,    # Merchants with total sales > 5000 and <= 10000 are mid value
}

# Outlier detection settings
OUTLIER_DETECTION = {
    "std_multiplier": 1.0,  # Number of standard deviations to consider as outlier
}

# Time granularity formats
TIME_FORMATS = {
    "daily": "%Y-%m-%d",
    "weekly": "%Y-W%W",
    "monthly": "%Y-%m",
    "yearly": "%Y",
}