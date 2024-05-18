from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from flask import Flask, render_template, redirect, url_for, request
from flask_login import LoginManager, UserMixin, login_user, logout_user, login_required, current_user
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.orm import relationship
from dotenv import load_dotenv
import pyodbc
import pandas as pd
import os

load_dotenv()

username = os.environ.get('USERNAME_AZURE')
password = os.environ.get('PASSWORD')
server = os.environ.get('SERVER')
database = os.environ.get('DATABASE')
account_storage = os.environ.get('ACCOUNT_STORAGE')
connection_string = "Driver={ODBC Driver 18 for SQL Server};"+f"Server=tcp:{server},1433;Database={database};Uid={username};Pwd={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;MultipleActiveResultSets = true;"

engine = create_engine(f'mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+18+for+SQL+Server')
try:
    # Attempt to connect to the database
    connection = pyodbc.connect(connection_string)
    print("Connected to the database successfully!")
    connection.close()
except Exception as e:
    print("Error connecting to the database:", e)
    
app = Flask(__name__)
app.secret_key = 'BigBeautifulServer'

# Initialize Flask-SQLAlchemy
app.config['SQLALCHEMY_DATABASE_URI'] = f"mssql+pyodbc://{os.environ.get('USERNAME_AZURE')}:{os.environ.get('PASSWORD')}@{os.environ.get('SERVER')}/{os.environ.get('DATABASE')}?driver=ODBC+Driver+18+for+SQL+Server"
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

login_manager = LoginManager()
login_manager.init_app(app)

# Mock user data
users = {'user1': {'password': 'password1', 'role': 'admin'},
         'user2': {'password': 'password2', 'role': 'viewer'}}

# Define the User model
class User(UserMixin, db.Model):
    __tablename__ = 'users'  # Adjust table name as needed

    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(100), unique=True, nullable=False)
    password = db.Column(db.String(100), nullable=False)

    def __repr__(self):
        return f"<User {self.username}>"
    
# Define the login manager loader
@login_manager.user_loader
def load_user(user_id):
    try:
        return User.query.get(int(user_id))
    except ValueError:
        return None
    
@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        if username in users and users[username]['password'] == password:
            user = User()
            user.id = username
            login_user(user)
            return redirect(url_for('dashboard'))
    return render_template('login.html')

# Logout route
@app.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('login'))

# Dashboard route
@app.route('/dashboard')
@login_required
def dashboard():
    return render_template('dashboard.html')

if __name__ == '__main__':
    app.run(debug=True)