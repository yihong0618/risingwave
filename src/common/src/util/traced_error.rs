// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::backtrace::{Backtrace, BacktraceStatus};

pub struct TracedInner<E> {
    error: E,
    backtrace: Backtrace,
}

impl<E> TracedInner<E>
where
    E: std::error::Error + 'static,
{
    pub fn new(error: E) -> Self {
        let requested = (&error as &dyn std::error::Error).request_ref::<Backtrace>();

        let backtrace = if requested.is_some() {
            Backtrace::disabled()
        } else {
            Backtrace::capture()
        };

        Self { error, backtrace }
    }

    pub fn causes(&self) -> impl Iterator<Item = &(dyn std::error::Error + 'static)> {
        (&self.error as &dyn std::error::Error).sources().skip(1)
    }

    pub fn backtrace(&self) -> Option<&Backtrace> {
        (self as &dyn std::error::Error).request_ref::<Backtrace>()
    }
}

impl<E> std::error::Error for TracedInner<E>
where
    E: std::error::Error + 'static,
    Self: std::fmt::Debug + std::fmt::Display,
{
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        E::source(&self.error)
    }

    fn provide<'a>(&'a self, demand: &mut std::any::Demand<'a>) {
        if let BacktraceStatus::Captured = self.backtrace.status() {
            demand.provide_ref::<Backtrace>(&self.backtrace);
        }
        E::provide(&self.error, demand);
    }
}

impl<E> std::fmt::Display for TracedInner<E>
where
    E: std::error::Error + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.error)?;

        if f.alternate() {
            for cause in self.causes() {
                write!(f, ": {}", cause)?;
            }
        }

        Ok(())
    }
}

impl<E> std::fmt::Debug for TracedInner<E>
where
    E: std::error::Error + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self)?;

        let mut causes = self.causes().peekable();

        if causes.peek().is_some() {
            write!(f, "\n\nCaused by:")?;
            for cause in causes {
                write!(f, "\n - {}", cause)?;
            }
        }

        if let Some(backtrace) = self.backtrace() {
            writeln!(f, "\n\nStack backtrace:\n{}", backtrace)?;
        }

        Ok(())
    }
}

#[macro_export]
macro_rules! define_traced_error {
    ($name:ident, $inner:ident, $doc:literal) => {
        #[doc = $doc]
        pub struct $name(Box<TracedInner<$inner>>);

        use $crate::util::traced_error::TracedInner;

        impl From<$inner> for $name {
            fn from(value: $inner) -> Self {
                Self(Box::new(TracedInner::new(value)))
            }
        }

        impl std::error::Error for $name {
            fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
                TracedInner::source(&*self.0)
            }

            fn provide<'a>(&'a self, demand: &mut std::any::Demand<'a>) {
                TracedInner::provide(&*self.0, demand)
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                TracedInner::fmt(&*self.0, f)
            }
        }

        impl std::fmt::Debug for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                TracedInner::fmt(&*self.0, f)
            }
        }
    };
}
