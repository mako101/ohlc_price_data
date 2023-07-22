import os
import sys
import pymysql
from importlib import util
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker

from api import FastAPI, Query
from fastapi_utils.inferring_router import InferringRouter
from api.openapi.utils import get_openapi
from api.openapi.docs import get_swagger_ui_html
from helpers import validate_date, validate_symbol, get_previous_date

# Import Stock object definition
spec = util.spec_from_file_location('model', 'models.py')
module = util.module_from_spec(spec)
sys.modules['model'] = module
spec.loader.exec_module(module)
from csv_processor.models import Stock

# FastAPI app and router
app = FastAPI()
router = InferringRouter(prefix='/api')


try:
    mysql_pwd = os.environ['MYSQL_PWD']
except KeyError:
    raise EnvironmentError('Please set `MYSQL_PWD` environmental variable')

mysql_db = f'mysql://root:{mysql_pwd}@mysql:3306/mydb'

pymysql.install_as_MySQLdb()

# Define the SQLAlchemy engine and session
engine = create_engine(mysql_db)
Session = sessionmaker(bind=engine)
session = Session()


# Route to retrieve stock records with customizable dates, limits, and pagination
@router.get("/financial_data")
def get_stock_records(
        symbol: str = Query(default='AAPL', description="Stock symbol acronym, eg `AAPL` or `IBM`"),
        start_date: str = Query(default=get_previous_date(14), description="Start date to query stock data, in the format YYYY-MM-DD"),
        end_date: str = Query(default=get_previous_date(0), description="End date to query stock data, in the format YYYY-MM-DD"),
        limit: int = Query(default=5, description="Amount of results per page to return"),
        page: int = Query(default=1, description="Page to read the data from")
):
    try:
        symbol = validate_symbol(symbol)
        query = session.query(Stock).filter(
            Stock.symbol == symbol,
            Stock.date >= validate_date(start_date),
            Stock.date <= validate_date(end_date)
        ).order_by(Stock.date.asc())

        total_records = query.count()
        total_pages = int(total_records // limit + (1 if total_records % limit > 0 else 0))

        records = query.limit(limit).offset((page - 1) * limit).all()

        if not records:
            return {
                "data": [],
                "pagination": {},
                "info": {'error': f'No records found for symbol {symbol}'}
            }

        return {
            "data": [record.__dict__ for record in records],
            "pagination": {
                "count": total_records,
                "page": page,
                "limit": limit,
                "pages": total_pages
            },
            "info": {'error': ''}
        }
    except Exception as e:
        return {
            "data": [],
            "pagination": {},
            "info": {'error': str(e)}
        }


# Route to perform basic statistical analysis on the stock records
@router.get("/statistics/")
def get_stock_records_statistics(
        symbol: str = Query(default='AAPL', description="Stock symbol acronym, eg `AAPL` or `IBM`"),
        start_date: str = Query(default=get_previous_date(14), description="Start date to query stock data, in the format YYYY-MM-DD"),
        end_date: str = Query(default=get_previous_date(0), description="End date to query stock data, in the format YYYY-MM-DD")
                                ):
    try:
        symbol = validate_symbol(symbol)
        query = session.query(
            func.avg(Stock.open_price).label('average_daily_open_price'),
            func.avg(Stock.close_price).label('average_daily_close_price'),
            func.avg(Stock.volume).label('average_daily_volume'),
        ).filter(
            Stock.symbol == symbol,
            Stock.date >= start_date,
            Stock.date <= end_date
        )

        result = query.first()
        if result == (None, None, None):
            return {
                "data": [],
                "info": {'error': f'No records found for symbol {symbol}'}
            }

        return {
            "data": {
                "start_date": start_date,
                "end_date": end_date,
                "symbol": symbol,
                "average_daily_open_price": float(f'{result.average_daily_open_price:.2f}'),
                "average_daily_close_price": float(f'{result.average_daily_close_price:.2f}'),
                "average_daily_volume": int(result.average_daily_volume)
            },
            "info": {'error': ''}
        }
    except Exception as e:
        return {
            "data": [],
            "info": {'error': str(e)}
        }


app.include_router(router)

# Endpoint to list available API endpoints
@app.get("/api")
def get_root():
    routes = []
    for route in app.routes:
        if isinstance(route, FastAPI) or route.path == "/":
            continue
        routes.append({"path": route.path, "methods": route.methods})
    return routes


# Swagger UI Endpoint
@app.get("/docs", include_in_schema=False)
def custom_swagger_ui_html():
    return get_swagger_ui_html(openapi_url="/openapi.json", title="API Docs")


# OpenAPI Schema endpoint
@app.get("/openapi.json", include_in_schema=False)
def get_open_api_endpoint():
    return get_openapi(title="API Docs", version="1.0.0", routes=app.routes)
