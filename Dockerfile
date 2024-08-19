# Use an official Python runtime as the base image
FROM python:3.11-slim-buster

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file and install dependencies
COPY application/requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy your application code into the container
COPY application .

# Set the command to run your application (replace 'app.py' with your main file)
CMD ["python", "app.py"]