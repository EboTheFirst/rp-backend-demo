def apply_merchant_date_filters(df, year=None, month=None, week=None, day=None, range_days=None, start_date=None, end_date=None):
    df["date"] = pd.to_datetime(df["date"])
    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month
    df["day"] = df["date"].dt.day
    df["week"] = df["date"].dt.isocalendar().week

    # Date range filters
    if start_date and end_date:
        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)
        df = df[(df["date"] >= start) & (df["date"] <= end)]
    elif range_days:
        end = pd.Timestamp.today().normalize()
        start = end - pd.Timedelta(days=range_days)
        df = df[(df["date"] >= start) & (df["date"] <= end)]

    # Date part filters
    if year:
        df = df[df["year"] == year]
    if month:
        df = df[df["month"] == month]
    if week:
        df = df[df["week"] == week]
    if day:
        df = df[df["day"] == day]

    return df
