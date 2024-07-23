# Setup
To Run the application i'd create a new python venv and activate it

Then once the venv is activated run: 
    pip install -r requirements.txt

Create a file called ".env" and copy what's inside dotenv.txt and in the ".env" file add your credentials (for the url it needs to be url:port)

Then you can run the application with > streamlit run app.py  in your terminal

app.py is the only file that truley is working with this front end right now, but the other files are useful for uploading and deleting files from the indicies 