use crate::{ParallelMap, ParallelMapBuilder, Scope};

pub struct ParallelFilterMapBuilder<I>(ParallelMapBuilder<I>)
where
    I: Iterator;

impl<I> ParallelFilterMapBuilder<I>
where
    I: Iterator,
{
    pub fn new(iter: I) -> Self {
        Self(ParallelMapBuilder::new(iter))
    }

    pub fn threads(self, num: usize) -> Self {
        Self(self.0.threads(num))
    }
    pub fn buffer_size(self, num: usize) -> Self {
        Self(self.0.buffer_size(num))
    }

    pub fn with<F, O>(self, f: F) -> ParallelFilterMap<'static, I, O>
    where
        I: Iterator + 'static,
        F: 'static + Send + Clone,
        O: Send + 'static,
        I::Item: Send + 'static,
        F: FnMut(I::Item) -> Option<O>,
    {
        ParallelFilterMap {
            iter: self.0.with(f),
        }
    }

    pub fn with_scoped<'env, 'scope, F, O>(
        self,
        scope: &'scope Scope<'scope, 'env>,
        f: F,
    ) -> ParallelFilterMap<'env, I, O>
    where
        I: Iterator + 'env,
        F: 'env + Send + Clone,
        O: Send + 'env,
        I::Item: Send + 'env,
        F: FnMut(I::Item) -> Option<O>,
    {
        ParallelFilterMap {
            iter: self.0.with_scoped(scope, f),
        }
    }
}

/// Like [`std::iter::FilterMap`] but multi-threaded
pub struct ParallelFilterMap<'a, I, O>
where
    I: Iterator,
{
    // the iterator we wrapped
    iter: ParallelMap<'a, I, Option<O>>,
}

impl<'a, I, O> Iterator for ParallelFilterMap<'a, I, O>
where
    I: Iterator,
    I::Item: Send,
    O: Send,
{
    type Item = O;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.iter.next() {
                Some(Some(item)) => return Some(item),
                Some(None) => continue,
                None => return None,
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }
}
