from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from models.file.FileRepo import FileRepo
from fastapi import Body



app = FastAPI()

origins = [
    "http://localhost:3000", 
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

fileRepo = FileRepo()

@app.get("/")
async def get_file():

    df = fileRepo.readFile(path="./data/base_de_vendas.xlsx")
    l = fileRepo.analyzeSales(dataframe=df, granularity='monthly', year="all")
    
    return l

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)