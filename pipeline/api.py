import os
from pathlib import Path

from fastapi import FastAPI,HTTPException
from fastapi.responses import HTMLResponse

from .engine import Pipeline


def create_app(path=None):
    db=Pipeline(path or os.environ.get('PIPELINE_DB','analytics.sqlite'))
    app=FastAPI(title='Incremental event analytics',version='0.1.0')

    @app.get('/health')
    def health():
        try:
            with db.connect() as connection: connection.execute('SELECT 1 FROM sources LIMIT 1')
        except Exception:raise HTTPException(503,'Analytical store unavailable') from None
        return {'status':'ok','service':'incremental-data-pipeline','version':'0.1.0'}

    @app.get('/analytics')
    def analytics():return db.analytics()

    @app.get('/',response_class=HTMLResponse)
    def home():return Path(__file__).with_name('index.html').read_text()

    return app


def main():
    import uvicorn
    uvicorn.run(create_app(),host=os.environ.get('HOST','127.0.0.1'),port=int(os.environ.get('PORT','8114')))


if __name__=='__main__':main()
