
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirement file into the container at /app
COPY ./requirements_docker.txt /app/requirements.txt

# Install any needed packages specified in requirements.txt
RUN pip3 install --no-cache-dir -r requirements.txt

# Run main when the container launches
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "80"]
