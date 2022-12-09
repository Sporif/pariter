use crate::{ParallelFilterMap, ParallelFilterMapBuilder, Scope};

pub struct ParallelFilterBuilder<I>(ParallelFilterMapBuilder<I>)
where
    I: Iterator;

impl<I> ParallelFilterBuilder<I>
where
    I: Iterator,
{
    pub fn new(iter: I) -> Self {
        Self(ParallelFilterMapBuilder::new(iter))
    }

    pub fn threads(self, num: usize) -> Self {
        Self(self.0.threads(num))
    }
    pub fn buffer_size(self, num: usize) -> Self {
        Self(self.0.buffer_size(num))
    }

    pub fn with<F>(self, mut f: F) -> ParallelFilter<'static, I>
    where
        I: Iterator + 'static,
        F: 'static + Send + Clone,
        I::Item: Send + 'static,
        F: FnMut(&I::Item) -> bool,
    {
        ParallelFilter {
            iter: self.0.with(move |v| if f(&v) { Some(v) } else { None }),
        }
    }

    pub fn with_scoped<'env, 'scope, F>(
        self,
        scope: &'scope Scope<'scope, 'env>,
        mut f: F,
    ) -> ParallelFilter<'env, I>
    where
        I: Iterator + 'env,
        F: 'env + Send + Clone,
        I::Item: Send + 'env,
        F: FnMut(&I::Item) -> bool + 'env + Send,
    {
        ParallelFilter {
            iter: self
                .0
                .with_scoped(scope, move |v| if f(&v) { Some(v) } else { None }),
        }
    }
}

/// Like [`std::iter::Filter`] but multi-threaded
pub struct ParallelFilter<'a, I>
where
    I: Iterator,
{
    // the iterator we wrapped
    iter: ParallelFilterMap<'a, I, I::Item>,
}

impl<'a, I> Iterator for ParallelFilter<'a, I>
where
    I: Iterator,
    I::Item: Send,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }
}
