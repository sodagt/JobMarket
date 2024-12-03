#FROM docker.elastic.co/kibana/kibana:8.1.2

# Copy exported dashboards to the container
#COPY expots/export.ndjson /usr/share/kibana/data/exported-dashboards.ndjson


#FROM tiangolo/uvicorn-gunicorn-fastapi:python3.7

#COPY ./app /app/app

FROM python:3.9-slim

WORKDIR /app

#COPY ./app /app

#pip install requests
#pip install flask_pydantic
#pip install pydantic
#pip install fastapi
#pip install python-multipart
#pip install --upgrade bcrypt

#RUN python3 -m venv venv

COPY ./requirements.txt /app/requirements.txt

#RUN pip3 install --upgrade bcrypt
RUN pip3 install --no-cache-dir -r requirements.txt
#EXPOSE 80

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "80"]



# Set the working directory in the container
#WORKDIR /app


#cleaning up docker image cache by using docker image prune -a -f

# Install any needed packages specified in requirements.txt
#RUN pip3 install --no-cache-dir -r requirements.txt


# Copy the current directory contents into the container at /app
#COPY ./app /code/app


# Make port 80 available to the world outside this container
#EXPOSE 80

# Define environment variable
#ENV NAME JobMarket

# Run app.py when the container launches
#CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "80"]
#CMD ["fastapi", "run", "app/main.py", "--proxy-headers", "--port", "80"]
#CMD ["fastapi", "run", "app/main.py", "--port", "80"]
