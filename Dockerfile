# Python image to use.
FROM python:3.10-slim-buster

ENV PYTHONBUFFERED 1
ENV TZ=Europe/Moscow



RUN python -m pip install --upgrade pip

RUN apt-get update -y && apt-get install -y gcc curl gnupg build-essential
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list
RUN apt-get update -y && apt-get install -y unixodbc unixodbc-dev tdsodbc freetds-common freetds-bin freetds-dev postgresql
RUN apt-get update && ACCEPT_EULA=Y apt-get -y install mssql-tools msodbcsql17
RUN echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bash_profile
RUN echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bashrc
RUN apt-get update




# Set the working directory to /app
WORKDIR /app

# copy the requirements file used for dependencies
COPY requirements.txt .


# Install any needed packages specified in requirements.txt
RUN python -m pip install --trusted-host pypi.python.org -r requirements.txt

#EXPOSE 8000
# Copy the rest of the working directory contents into the container at /app
COPY . .

# Run main.py when the container launches
CMD ["python", "main.py"]
#cat /etc/os-release