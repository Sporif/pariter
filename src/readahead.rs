use flume::{bounded, Receiver, Sender};

use crate::Scope;
use std::{
    marker::PhantomData,
    sync::{atomic::AtomicBool, Arc},
    thread,
};

use crate::DropIndicator;

pub struct ReadaheadBuilder<I>
where
    I: Iterator,
{
    // the iterator we wrapped
    iter: I,
    // max number of items in flight
    buffer_size: Option<usize>,
    // Runtime disabling of readahead
    serial: bool,
}

impl<I> ReadaheadBuilder<I>
where
    I: Iterator,
{
    pub fn new(iter: I) -> Self {
        Self {
            iter,
            buffer_size: None,
            serial: false,
        }
    }

    pub fn buffer_size(self, num: usize) -> Self {
        Self {
            buffer_size: Some(num),
            ..self
        }
    }

    pub fn serial(self, val: bool) -> Self {
        Self {
            serial: val,
            ..self
        }
    }

    fn with_common(self) -> (Readahead<I>, Sender<I::Item>, I, bool)
    where
        I: Iterator,
    {
        let buffer_size = self.buffer_size.unwrap_or(0);

        let (tx, rx) = bounded(buffer_size);
        (
            Readahead {
                iter: None,
                _iter_marker: PhantomData,
                iter_size_hint: self.iter.size_hint(),
                inner: Some(ReadaheadInner { rx }),
                worker_panicked: Arc::new(AtomicBool::new(false)),
            },
            tx,
            self.iter,
            self.serial,
        )
    }

    pub fn with(self) -> Readahead<I>
    where
        I: Iterator + 'static + Send,
        I::Item: Send + 'static,
    {
        let (mut ret, tx, mut iter, serial) = self.with_common();

        if serial {
            ret.iter = Some(iter)
        } else {
            let drop_indicator = DropIndicator::new(ret.worker_panicked.clone());
            thread::spawn(move || {
                for i in iter.by_ref() {
                    // don't panic if the receiver disconnects
                    let _ = tx.send(i);
                }
                drop_indicator.cancel();
            });
        }

        ret
    }

    pub fn with_scoped<'env, 'scope>(self, scope: &'scope Scope<'scope, 'env>) -> Readahead<I>
    where
        I: Iterator + 'env + Send,
        I::Item: Send + 'env,
    {
        let (mut ret, tx, mut iter, serial) = self.with_common();

        if serial {
            ret.iter = Some(iter)
        } else {
            let drop_indicator = DropIndicator::new(ret.worker_panicked.clone());
            scope.spawn(move || {
                for i in iter.by_ref() {
                    // don't panic if the receiver disconnects
                    let _ = tx.send(i);
                }
                drop_indicator.cancel();
            });
        }

        ret
    }
}
/// And iterator that provides parallelism
/// by running the inner iterator in another thread.
pub struct Readahead<I>
where
    I: Iterator,
{
    iter: Option<I>,
    _iter_marker: PhantomData<I>,
    iter_size_hint: (usize, Option<usize>),
    inner: Option<ReadaheadInner<I>>,
    worker_panicked: Arc<AtomicBool>,
}

struct ReadaheadInner<I>
where
    I: Iterator,
{
    rx: Receiver<I::Item>,
}

impl<I> Iterator for Readahead<I>
where
    I: Iterator,
    I: Send,
    I::Item: Send,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(iter) = &mut self.iter {
            iter.next()
        } else {
            match self.inner.as_ref().expect("thread started").rx.recv() {
                Ok(i) => Some(i),
                Err(_e) => {
                    if self
                        .worker_panicked
                        .load(std::sync::atomic::Ordering::SeqCst)
                    {
                        panic!("readahead worker thread panicked: panic indicator set");
                    } else {
                        None
                    }
                }
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter_size_hint
    }
}
