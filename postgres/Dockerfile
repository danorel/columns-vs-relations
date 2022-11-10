FROM python:3.8-alpine

# The latest alpine images don't have some tools like (`git` and `bash`).
# Adding git, bash and openssh to the image
RUN apk update && apk upgrade && \
    apk add --no-cache python3-dev bash git openssh gcc g++

# Make dir app
RUN mkdir /app
WORKDIR /app

# Copy the source from the current directory to the Working Directory inside the container
COPY * /app/

# Install dependencies
RUN pip3 install -r requirements.txt

# Expose port 8080 to the outside world
EXPOSE 8080

# Run the executable
CMD ["python3", "main.py"]