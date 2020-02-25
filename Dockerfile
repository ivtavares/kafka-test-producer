# Start with a base image
FROM python:3.7.4
# Copy our application code
WORKDIR /var/app
COPY . .
COPY requirements.txt .
# Fetch app specific dependencies
RUN pip install -r requirements.txt
# Start the app
CMD [ "python", "/var/app/app.py" ]
