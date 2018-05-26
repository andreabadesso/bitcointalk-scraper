FROM alpine:3.1

RUN apk add --update python py-pip

# Bundle app source
COPY ../../ .

RUN pip install -r ./requirements.txt

EXPOSE  8000
CMD ["python", "topic_alert.py"]
