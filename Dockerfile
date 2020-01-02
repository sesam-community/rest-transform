FROM python:3-alpine
MAINTAINER Baard H. Rehn Johansen "baard@rehn.no"
ARG BuildNumber=unknown
LABEL BuildNumber $BuildNumber
ARG Commit=unknown
LABEL Commit $Commit

COPY ./service /service

WORKDIR /service
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

EXPOSE 5000/tcp
ENTRYPOINT ["python"]
CMD ["transform-service.py"]
