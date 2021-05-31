from elasticsearch import Elasticsearch
from redis import Redis
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView
from rq import Queue
from rq.exceptions import NoSuchJobError
from rq.job import Job

from simple_indexing_service import settings

redis = Redis()
q = Queue(connection=redis)
es = Elasticsearch(
    cloud_id=settings.CLOUD_ID,
    http_auth=("elastic", settings.ES_PASSWORD),
)


def es_bulk(body):
    return es.bulk(body=body, refresh="wait_for")


class DocksView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request):
        """
            Accepts batches of documents from user to save them in a user-specific index.
            Returns an id of a queued job.
        """
        body = []
        if (
                not isinstance(request.data, list)
                or not all(isinstance(source, dict) for source in request.data)
        ):
            return Response(status=status.HTTP_400_BAD_REQUEST)
        for source in request.data:
            body.append(
                {
                    "index": {
                        "_index": f"zeta_alpha_{request.user}",
                    }
                }
            )
            body.append(source)
        job = q.enqueue(es_bulk, kwargs={"body": body})
        return Response({"job_id": job.id}, status=status.HTTP_200_OK)


class JobsView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        """
            Accepts job_id as a query_param.
            Returns its status and (if applicable) the result.
        """
        try:
            job = Job.fetch(request.query_params["job_id"], connection=redis)
        except NoSuchJobError:
            return Response(status=status.HTTP_404_NOT_FOUND)
        except KeyError:
            return Response(status=status.HTTP_400_BAD_REQUEST)
        return Response(
            {
                "status": job.get_status(),
                "result": job.result,
            },
            status=status.HTTP_200_OK
        )


class SearchView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request):
        """
            Accepts search requests written in elasticsearch query dsl.
            Looks up only in user-specific index.
        """
        result = es.search(
            body=request.data,
            index=f"zeta_alpha_{request.user}",
        )
        return Response(
            result,
            status=status.HTTP_200_OK
        )
