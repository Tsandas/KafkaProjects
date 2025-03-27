import yfinance as yf
from kafka import KafkaProducer
import logging


def get_stock_price(ticker):
    stock = yf.Ticker(ticker)
    try:
        price = stock.fast_info["last_price"]
        return price
    except KeyError:
        print(f"Could not retrieve data for {ticker}")


def on_send_success(record_metadata):
    print(record_metadata.topic)
    print(record_metadata.partition)
    print(record_metadata.offset)


def on_send_error(excp):
    logging.error('Unexpected error', exc_info=excp)


if __name__ == "__main__":

    producer = KafkaProducer(bootstrap_servers='localhost:9092',
                             key_serializer=str.encode,
                             value_serializer=lambda v: str(v).encode('utf-8')
                             )

    ticker_symbol = "AAPL"
    stock_price = get_stock_price(ticker_symbol)

    if stock_price is not None:
        producer.send('topics.test.v1',
                      key=ticker_symbol,
                      value=stock_price).add_callback(on_send_success).add_errback(on_send_error)

        producer.flush()
        producer.close()
    else:
        print("Failed to send message")