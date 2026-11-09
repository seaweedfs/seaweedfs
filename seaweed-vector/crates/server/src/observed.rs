use crate::metrics::Metrics;
use crate::pb::vector::{self as pb, vector_service_server::VectorService};
use crate::service::VectorServer;
use tonic::{Request, Response, Status};

pub struct ObservedService {
    pub inner: VectorServer,
    pub metrics: Metrics,
}

#[tonic::async_trait]
impl VectorService for ObservedService {
    async fn create_index(
        &self,
        request: Request<pb::CreateIndexRequest>,
    ) -> Result<Response<pb::CreateIndexResponse>, Status> {
        let mut in_flight = self.metrics.start("CreateIndex");
        let result = self.inner.create_index(request).await;
        in_flight.complete(result.as_ref().err().map(|e| e.code()));
        result
    }
    async fn drop_index(
        &self,
        request: Request<pb::DropIndexRequest>,
    ) -> Result<Response<pb::DropIndexResponse>, Status> {
        let mut in_flight = self.metrics.start("DropIndex");
        let result = self.inner.drop_index(request).await;
        in_flight.complete(result.as_ref().err().map(|e| e.code()));
        result
    }
    async fn put_vectors(
        &self,
        request: Request<pb::PutVectorsRequest>,
    ) -> Result<Response<pb::PutVectorsResponse>, Status> {
        let mut in_flight = self.metrics.start("PutVectors");
        let result = self.inner.put_vectors(request).await;
        in_flight.complete(result.as_ref().err().map(|e| e.code()));
        result
    }
    async fn get_vectors(
        &self,
        request: Request<pb::GetVectorsRequest>,
    ) -> Result<Response<pb::GetVectorsResponse>, Status> {
        let mut in_flight = self.metrics.start("GetVectors");
        let result = self.inner.get_vectors(request).await;
        in_flight.complete(result.as_ref().err().map(|e| e.code()));
        result
    }
    async fn delete_vectors(
        &self,
        request: Request<pb::DeleteVectorsRequest>,
    ) -> Result<Response<pb::DeleteVectorsResponse>, Status> {
        let mut in_flight = self.metrics.start("DeleteVectors");
        let result = self.inner.delete_vectors(request).await;
        in_flight.complete(result.as_ref().err().map(|e| e.code()));
        result
    }
    async fn list_vectors(
        &self,
        request: Request<pb::ListVectorsRequest>,
    ) -> Result<Response<pb::ListVectorsResponse>, Status> {
        let mut in_flight = self.metrics.start("ListVectors");
        let result = self.inner.list_vectors(request).await;
        in_flight.complete(result.as_ref().err().map(|e| e.code()));
        result
    }
    async fn query_vectors(
        &self,
        request: Request<pb::QueryVectorsRequest>,
    ) -> Result<Response<pb::QueryVectorsResponse>, Status> {
        let mut in_flight = self.metrics.start("QueryVectors");
        let result = self.inner.query_vectors(request).await;
        in_flight.complete(result.as_ref().err().map(|e| e.code()));
        if let Ok(response) = &result {
            self.metrics
                .query_results
                .inc_by(response.get_ref().vectors.len() as u64);
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn failures_are_counted_and_release_in_flight_requests() {
        let service = ObservedService {
            inner: VectorServer::new(),
            metrics: Metrics::new().unwrap(),
        };
        let response = service
            .query_vectors(Request::new(pb::QueryVectorsRequest::default()))
            .await;
        let code = format!("{:?}", response.unwrap_err().code());
        assert_eq!(
            service
                .metrics
                .requests
                .with_label_values(&["QueryVectors", &code])
                .get(),
            1
        );
        assert_eq!(
            service
                .metrics
                .in_flight
                .with_label_values(&["QueryVectors"])
                .get(),
            0
        );
        assert_eq!(
            service
                .metrics
                .request_seconds
                .with_label_values(&["QueryVectors"])
                .get_sample_count(),
            1
        );
    }
}
