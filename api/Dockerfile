# Use official lightweight Python image as base OS.
FROM python:3.8-slim
# Copy local code to the container image.
ENV APP_HOME /app
WORKDIR $APP_HOME
COPY . ./
# Install production dependencies.
RUN pip install -r ./requirements.txt
ENV PORT 8080

ENTRYPOINT [ "python" ]
CMD ["main.py"]
