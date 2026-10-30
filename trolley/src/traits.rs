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
