use std::fmt;

use async_trait::async_trait;

#[async_trait]
pub trait Service<Input>: fmt::Debug
where
    Input: fmt::Debug,
{
    type Output: fmt::Debug;
    async fn call(&self, input: Input) -> Self::Output;
}

#[async_trait]
pub trait ServiceMut<Input>: fmt::Debug
where
    Input: fmt::Debug,
{
    type Output: fmt::Debug;
    async fn call(&mut self, input: Input) -> Self::Output;
}

#[async_trait]
pub trait InfallibleService<Input>: fmt::Debug
where
    Input: fmt::Debug,
{
    type Output: fmt::Debug;
    async fn call(&self, input: Input) -> Self::Output;
}

#[async_trait]
pub trait InfallibleServiceMut<Input>: fmt::Debug
where
    Input: fmt::Debug,
{
    type Output: fmt::Debug;
    async fn call(&mut self, input: Input) -> Self::Output;
}

pub trait GrpcService<Input>
where
    Input: fmt::Debug,
    Self: Service<Input>,
    Self::ProtoRsp: From<<Self as Service<Input>>::Output>,
{
    type ProtoReq: Into<Input>;
    type ProtoRsp;
    // async fn rpc_call(&self, req: tonic::Request<<Input as RpcProtoMsg>::Message>) -> Result<tonic::Response<<
}
