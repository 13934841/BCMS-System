from fastapi import FastAPI, Depends, HTTPException, status, Response, Form, Request
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, RedirectResponse
from jose import jwt, JWTError
from passlib.context import CryptContext
from pydantic import BaseModel
from dotenv import load_dotenv
import os
import json
from datetime import datetime, timedelta
import pyodbc
from datasetup import AzureDB

# Load environment variables
load_dotenv()

# Environment variables
username = os.environ.get('USERNAME_AZURE')
password = os.environ.get('PASSWORD')
server = os.environ.get('SERVER')
database_name = os.environ.get('DATABASE')
account_storage = os.environ.get('ACCOUNT_STORAGE')
connection_string = (
    "Driver={ODBC Driver 18 for SQL Server};"
    f"Server=tcp:{server},1433;Database={database_name};"
    f"Uid={username};Pwd={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
    "MultipleActiveResultSets=true;"
)

# Initialize AzureDB instance
database = AzureDB()

# Password context
pwd_context = CryptContext(schemes=["argon2"], deprecated="auto")

# Security settings
SECRET_KEY = "BigBeautifulServer"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

# Initialize FastAPI app
app = FastAPI()

# Allow all origins
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
)

# OAuth2 password bearer
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

# In-memory users database
users_db = {
    "manager1": {
        "username": "manager1",
        "full_name": "Manager One",
        "hashed_password": pwd_context.hash("managerpass"),
        "roles": ["manager"],
        "id": 0,
    },
    "john": {
        "username": "john",
        "full_name": "John Smith",
        "hashed_password": pwd_context.hash("1234"),
        "roles": ["employee"],
        "id": 1,
    },
    "bob": {
        "username": "bob",
        "full_name": "Bob Wong",
        "hashed_password": pwd_context.hash("1234"),
        "roles": ["employee"],
        "id": 2,
    },
    "ann": {
        "username": "ann",
        "full_name": "Ann Li",
        "hashed_password": pwd_context.hash("1234"),
        "roles": ["employee"],
        "id": 3,
    },
}

# SQL database access
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
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM StaffDim WHERE StaffID = ?", (username,))
        user = cursor.fetchone()
        if user and user.Password == password:
            cursor.execute("SELECT RoleID FROM StaffDim WHERE StaffID = ?", (username,))
            role = cursor.fetchone()
            if role:
                cursor.execute("SELECT Permission FROM RoleDim WHERE RoleID = ?", (role[0],))
                permission = cursor.fetchone()
                user_permission = permission[0] if permission else None
                user_data = {
                    "username": user.StaffID,
                    "permission": user_permission,
                }
                return user_data
        return None
    except Exception as e:
        print(f"Error during user authentication: {e}")
        return None
    finally:
        conn.close()

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
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends()):
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
        expires_delta=access_token_expires,
    )
    return {"access_token": access_token, "token_type": "bearer"}

@app.get("/inspect_token")
async def inspect_token(token: str):
    try:
        decoded_token = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return decoded_token
    except JWTError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

# Define permission levels
PERMISSION_READ = "Read"
PERMISSION_READ_WRITE = "Read/Write"
PERMISSION_FULL_ACCESS = "Full Access"

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
        user_permission = user_data.get("permission")
        return User(username=username, roles=user_data['roles'], id=user_data['id'])
    except JWTError:
        raise credentials_exception

def check_user_role(role: str):
    def role_checker(current_user: User = Depends(get_current_user)):
        if role not in current_user.roles:
            raise HTTPException(status_code=403, detail="Insufficient permissions")
        return current_user
    return role_checker

@app.get("/")
async def hello():
    return json.dumps({"data": "This is common data available to all authenticated users"})

@app.get("/data/common")
async def read_common_data(response: Response, current_user: User = Depends(get_current_user)):
    response = add_cors_headers(response)
    return json.dumps({"data": "This is common data available to all authenticated users"})

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
    user_data = authenticate_user(username, password)
    if user_data:
        permission = user_data["permission"]
        if permission == PERMISSION_READ:
            return RedirectResponse(url="https://app.powerbi.com/groups/me/reports/0f517cdb-8e59-4552-a8e6-38b8437ae77a/ReportSection?experience=power-bi")
        elif permission == PERMISSION_READ_WRITE:
            return RedirectResponse(url="https://app.powerbi.com/groups/me/reports/04166c59-d309-4467-b2fc-b1df2f49189c/ReportSection?experience=power-bi")
        elif permission == PERMISSION_FULL_ACCESS:
            return RedirectResponse(url="https://app.powerbi.com/groups/me/reports/04166c59-d309-4467-b2fc-b1df2f49189c/ReportSection?experience=power-bi")
        else:
            return "Unknown permission level"
    else:
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

@app.get("/read_dashboard", response_class=HTMLResponse)
@app.post("/read_dashboard", response_class=HTMLResponse)
async def read_dashboard(request: Request, current_user: User = Depends(get_current_user)):
    return HTMLResponse(content="<h1>Welcome to the Read-Only Dashboard!</h1>")

@app.get("/read_write_dashboard", response_class=HTMLResponse)
@app.post("/read_write_dashboard", response_class=HTMLResponse)
async def read_write_dashboard(request: Request, current_user: User = Depends(get_current_user)):
    return HTMLResponse(content="<h1>Welcome to the Read/Write Dashboard!</h1>")

@app.get("/full_access_dashboard", response_class=HTMLResponse)
@app.post("/full_access_dashboard", response_class=HTMLResponse)
async def full_access_dashboard(request: Request, current_user: User = Depends(get_current_user)):
    return HTMLResponse(content="<h1>Welcome to the Full Access Dashboard!</h1>")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, port=8000)
