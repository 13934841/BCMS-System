from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import jwt, JWTError
from passlib.context import CryptContext
from datetime import datetime, timedelta
from pydantic import BaseModel
from dotenv import load_dotenv
import os
from fastapi.middleware.cors import CORSMiddleware
import json
from fastapi import Response
from fastapi.responses import HTMLResponse
from fastapi import Form
from utils.datasetup import AzureDB
from datasetup import AzureDB
import pyodbc
from starlette.requests import Request
from datasetup import engine
from sqlalchemy import text, create_engine, MetaData, Table, Column, Integer, String
import pandas as pd

load_dotenv()

username = os.environ.get('USERNAME_AZURE')
password = os.environ.get('PASSWORD')
server = os.environ.get('SERVER')
database = os.environ.get('DATABASE')
account_storage = os.environ.get('ACCOUNT_STORAGE')
connection_string = "Driver={ODBC Driver 18 for SQL Server};"+f"Server=tcp:{server},1433;Database={database};Uid={username};Pwd={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;MultipleActiveResultSets = true;"

# Initialize AzureDB instance
database = AzureDB()

# Password context
pwd_context = CryptContext(schemes=["argon2"], deprecated="auto")

# Security
SECRET_KEY = "BigBeautifulServer"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

app = FastAPI()

# List of allowed origins
origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"]
)

# Password context
pwd_context = CryptContext(schemes=["argon2"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

# Database simulation
users_db = {
    "manager1": {
        "username": "manager1",
        "full_name": "Manager One",
        "hashed_password": pwd_context.hash("managerpass"),
        "roles": ["manager"],
        "id": 0
    },
    "john": {
        "username": "john",
        "full_name": "John Smith",
        "hashed_password": pwd_context.hash("1234"),
        "roles": ["employee"],
        "id": 1
    },
    "bob": {
        "username": "bob",
        "full_name": "Bob Wong",
        "hashed_password": pwd_context.hash("1234"),
        "roles": ["employee"],
        "id": 2
    },
    "ann": {
        "username": "ann",
        "full_name": "Ann Li",
        "hashed_password": pwd_context.hash("1234"),
        "roles": ["employee"],
        "id": 3
    }
}

# SQL database access
database=AzureDB()
database.access_container("csv-files")

class Token(BaseModel):
    access_token: str
    token_type: str

class User(BaseModel):
    username: str
    roles: list
    id: int
    
def authenticate_user(username: str, password: str):
    try:
        # Establish a connection to the database
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()

        # Query the database for the user with the provided username
        cursor.execute("SELECT * FROM StaffDim WHERE StaffID = ?", (username,))
        user = cursor.fetchone()

        # Check if the user exists and the password matches
        if user and user.Password == password:
            return user  # Return the user details
        else:
            return False  # Authentication failed

    except Exception as e:
        print(f"Error during user authentication: {e}")
        return False  # Authentication failed
    finally:
        # Close the database connection
        conn.close()
        
# def insert_login_record(staff_id: int, ip: str, login_time: datetime):
#     # Convert login_time to the expected format
#     lt = login_time.timestamp()
#     lt = pd.to_datetime(lt, unit='s')
#     # Construct the SQL query with the properly formatted datetime string
#     query = f"INSERT INTO LoginRecordDim (StaffID, IP, LoginTime) VALUES ({staff_id}, '{ip}', '{lt}')"
#     # Execute the query
#     with engine.connect() as con:
#         con.execute(text(query))

# def insert_login_record(staff_id: int, ip: str):
#     # Construct the SQL query without the LoginRecordID column
#     query = "INSERT INTO LoginRecordDim (StaffID, IP) VALUES (:staff_id, :ip)"
#     # Execute the query
#     with engine.connect() as con:
#         con.execute(text(query), {"staff_id": staff_id, "ip": ip})




# def insert_login_record(staff_id: int, ip: str):
#     query = "INSERT INTO LoginRecordDim (StaffID, IP) OUTPUT INSERTED.LoginRecordID VALUES (?, ?)"
#     params = (staff_id, ip)
#     try:
#         with engine.connect() as con:
#             result = con.execute(query, params)
#             login_record_id = result.scalar()  # Get the inserted LoginRecordID
#             print(f"Inserted login record with LoginRecordID: {login_record_id}")
#     except Exception as e:
#         print(f"Error inserting login record: {e}")




def create_access_token(data: dict, expires_delta: timedelta = None):
    to_encode = data.copy()
    expire = datetime.utcnow() + expires_delta if expires_delta else datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

def add_cors_headers(response: Response):
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Methods'] = 'POST, GET, OPTIONS'
    response.headers['Access-Control-Allow-Headers'] = 'Authorization, Content-Type'
    return response

@app.post("/token", response_model=Token)
async def login_for_access_token(response: Response, form_data: OAuth2PasswordRequestForm = Depends()):
    response = add_cors_headers(response)   
    user = authenticate_user(form_data.username, form_data.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": user['username'], "roles": user['roles'], "id": user['id'], "name": user['full_name']},
        expires_delta=access_token_expires
    )
    return {"access_token": access_token, "token_type": "bearer"}

async def get_current_user(token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
        user_data = users_db.get(username, None)
        if user_data is None:
            raise credentials_exception
        return User(username=user_data['username'], roles=user_data['roles'], id=user_data['id'])
    except JWTError:
        raise credentials_exception

def check_user_role(role: str):
    def role_checker(current_user: User = Depends(get_current_user)):
        if role not in current_user.roles:
            raise HTTPException(status_code=403, detail="Insufficient permissions")
        return current_user
    return role_checker

# API endpoints
@app.get("/")
async def hello():
    return json.dumps({"data": "This is common data available to all authenticated users"})

@app.get("/data/common")
async def read_common_data(response: Response, current_user: User = Depends(get_current_user)):
    response = add_cors_headers(response) 
    return json.dumps({"data": "This is common data available to all authenticated users"})


from fastapi import Request

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse

from enum import Enum

# Define permissions
class Permission(Enum):
    READ = 1
    READ_WRITE = 2
    FULL_ACCESS = 3

def get_permission_from_database(role_id):
    # Query the database to get the permission associated with the given role_id
    # Assuming you have a database connection established and a cursor created
    
    # Example using SQLAlchemy:
    # Replace 'your_table_name' with the actual table name
    with engine.connect() as con:
        query = f"SELECT Permission FROM RoleDim WHERE RoleID = {role_id}"
        con.execute(query).fetchone()[0]

    # # Example hardcoded logic based on the provided table structure
    # if role_id == 'ANA':
    #     return Permission.READ
    # elif role_id == 'MAN':
    #     return Permission.READ_WRITE
    # elif role_id == 'ADM':
    #     return Permission.FULL_ACCESS
    # else:
    #     return Permission.READ  # Default to read-only if role not found

@app.get("/login", response_class=HTMLResponse)
async def login_page():
    return """
    <h1>Login</h1>
    <form method="post">
    <label for="username">Username</label>
    <input type="text" id="username" name="username"><br>
    <label for="password">Password</label>
    <input type="password" id="password" name="password"><br>
    <button type="submit">Login</button>
    </form>
    """

@app.post("/login", response_class=HTMLResponse)
async def login_submit(request: Request, username: str = Form(...), password: str = Form(...)):
    user = authenticate_user(username, password)
    if user:
        # Authentication successful, redirect to a different page or render a success message
        return HTMLResponse(content="<h1>Login successful!</h1>")
    else:
        # Authentication failed, render the login page with an error message
        return """
        <h1>Login</h1>
        <p style="color: red;">Incorrect username or password</p>
        <form method="post">
        <label for="username">Username</label>
        <input type="text" id="username" name="username"><br>
        <label for="password">Password</label>
        <input type="password" id="password" name="password"><br>
        <button type="submit">Login</button>
        </form>
        """
        
# @app.post("/login", response_class=HTMLResponse)
# async def login_page(request: Request, username: str = Form(...), password: str = Form(...)):
#     # Get the client's IP address
#     ip = request.client.host
#     # Authenticate user
#     user = authenticate_user(username, password)
#     if not user:
#         # Handle authentication failure
#         raise HTTPException(
#             status_code=status.HTTP_401_UNAUTHORIZED,
#             detail="Incorrect username or password",
#             headers={"WWW-Authenticate": "Bearer"},
#         )
#     else:
#         # Extract user ID from the user object
#         user_id = user[0]  # Assuming StaffID is the first column in the query result
#         # Insert login record
#         insert_login_record(user_id, ip)#, datetime.now())
#         # Authenticate successful login
#         # Return login page or redirect to dashboard







# Running the app with Uvicorn
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, port=8000)