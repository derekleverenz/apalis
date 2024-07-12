use std::{
    pin::Pin,
    task::{Context, Poll},
    time::Instant,
};

use apalis_core::{error::Error, request::Request, storage::Job};
use futures::Future;
use metrics::{Counter, Histogram};
use pin_project_lite::pin_project;
use tower::{Layer, Service};

/// A layer to support prometheus metrics
#[derive(Debug, Default)]
pub struct PrometheusLayer {
    // storing this here at creation time, but we could potentially get
    // the job type and name by capturing the job type in a phantom data field
    // and grabbing the typename or NAME from it
    job_name: String,
}

impl PrometheusLayer {
    /// Create a new PrometheusLayer that instruments metrics with a lable of the specified job
    /// name
    pub fn new(job_name: &str) -> Self {
        Self {
            job_name: job_name.to_string(),
        }
    }
}

impl<S> Layer<S> for PrometheusLayer {
    type Service = PrometheusService<S>;

    fn layer(&self, service: S) -> Self::Service {
        let labels = [("job_name", self.job_name.clone())];

        let req_counter = metrics::counter!("apalis_requests_total", &labels);
        let req_histogram = metrics::histogram!("apalis_request_duration_seconds", &labels);

        PrometheusService {
            service,
            req_counter,
            req_histogram,
        }
    }
}

/// This service implements the metric collection behavior
#[derive(Clone)]
pub struct PrometheusService<S> {
    service: S,
    req_counter: Counter,
    req_histogram: Histogram,
}

// manually implement debug because the metric structs do not have a Debug implementation
impl<S> std::fmt::Debug for PrometheusService<S>
where
    S: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PrometheusService")
            .field("service", &self.service)
            .finish()
    }
}

impl<S, J, F, Res> Service<Request<J>> for PrometheusService<S>
where
    S: Service<Request<J>, Response = Res, Error = Error, Future = F>,
    F: Future<Output = Result<Res, Error>> + 'static,
    J: Job,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = ResponseFuture<F>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&mut self, request: Request<J>) -> Self::Future {
        let start = Instant::now();
        let req = self.service.call(request);
        ResponseFuture {
            inner: req,
            start,
            req_counter: self.req_counter.clone(),
            req_histogram: self.req_histogram.clone(),
        }
    }
}

pin_project! {
    /// Response for prometheus service
    pub struct ResponseFuture<F> {
        #[pin]
        pub(crate) inner: F,
        pub(crate) start: Instant,
        pub(crate) req_counter: Counter,
        pub(crate) req_histogram: Histogram,
    }
}

impl<Fut, Res> Future for ResponseFuture<Fut>
where
    Fut: Future<Output = Result<Res, Error>>,
{
    type Output = Result<Res, Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let response = futures::ready!(this.inner.poll(cx));

        let latency = this.start.elapsed().as_secs_f64();

        this.req_counter.increment(1);
        this.req_histogram.record(latency);

        Poll::Ready(response)
    }
}
