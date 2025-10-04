import yfinance as yf

def fetch_ticker_data(ticker: str, start: str = "2018-01-01"):
    """
    Fetch historical market data from Yahoo Finance for a given ticker symbol.

    Returns:
    - pd.DataFrame: A DataFrame containing the historical market data.
    """
    df = yf.download(ticker, start=start)

    df = df.reset_index().rename(columns=str.lower)  # padroniza
    
    df["ticker"] = ticker

    return df