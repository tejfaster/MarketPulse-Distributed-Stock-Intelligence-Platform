import json 
import time
import logging
from datetime import datetime
from kafka import KafkaProducer
import yfinance as yf

# Logging Setup
logging.basicConfig(
    level=logging.INFO,
    format = '%(asctime)s [%(levelname)s] %(message)s'
)

logger = logging.getLogger(__name__)

# Config 
KAFKA_BROKER = '10.95.208.20:9092'
KAFKA_TOPIC = 'stock-prices'
INTERVAL_SEC = 10

STOCKS = [
    # US Tech
    'AAPL','GOOGL','MSFT','AMZN','TSLA','NVDA','META',
    # Indian
    'RELIANCE.NS','TCS.NS','INFY.NS','HDFCBANK.NS',
    # Crypto
    'BTC-USD','ETH-USD','BNB-USD',
    # Indices
    '^NSEI','^GSPC','^DJI'
]

# Kafka producer
producer = KafkaProducer(
    bootstrap_servers = KAFKA_BROKER,
    value_serializer = lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8')                                                                                
)

# fetching stock data

def fetch_stock_data(symbol):
    try:
        ticker = yf.Ticker(symbol)
        data = ticker.fast_info

        record = {
            'symbol' : symbol,
            'price' : round(data.last_price,4),
            'prev_close' : round(data.previous_close,4),
            'change' : round(data.last_price - data.previous_close,4),
            'change_pct' : round((data.last_price - data.previous_close) / data.previous_close * 100,4),
            'volume' : data.three_month_average_volume,
            'market_cap' : data.market_cap,
            'timestamp' : datetime.utcnow().isoformat(),
            'exchange' : data.exchange    
        }

        return record
    
    except Exception as e:
        logger.error(f"Failed to fetch {symbol}:{e}")
        return None



# Main Producer loop

def produce():
    logger.info("Starting MarketPluse Producer")
    logger.info(f"Broker:{KAFKA_BROKER}")
    logger.info(f"Topic:{KAFKA_TOPIC}")
    logger.info(f"Symbols:{len(STOCKS)} stocks")
    logger.info(f"Interval:{INTERVAL_SEC}s")
    logger.info("-" * 50)

    while True:
        for symbol in STOCKS:
            record = fetch_stock_data(symbol)

            if record:
                producer.send(
                    topic=KAFKA_TOPIC,
                    key=symbol,
                    value=record
                )
                logger.info(
                    f"{symbol:10s} |"
                    f"Price: ${record['price']:>10.2f} |"
                    f"Change: {record['change_pct']:>+6.2f}%"
                )
        
        producer.flush()       
        logger.info(f"Waiting {INTERVAL_SEC}s...\n")
        time.sleep(INTERVAL_SEC)

# Entry Point

if __name__ == '__main__':
    try:
        produce()
    except KeyboardInterrupt:
        logger.info("Producer stopped")
        produce.close()   

