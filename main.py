from fastapi import FastAPI

from worker.agent import *

app = FastAPI()


@app.get('/')
def main():
    return {'main': 'page'}


