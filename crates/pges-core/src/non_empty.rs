//! Lists that hold at least one item.

use std::{iter, num::NonZeroUsize, slice, vec};

/// A list with at least one item, for options where an empty list is a mistake: writer roles, endpoints,
/// wakers, publications.
///
/// Build one from known items with [`non_empty!`](crate::non_empty!) or [`NonEmpty::new`], which cannot be
/// empty, or from a list known only at run time with `TryFrom<Vec<T>>` or [`NonEmpty::try_from_iter`].
///
/// ```
/// use pges_core::non_empty;
///
/// let roles = non_empty!["app_writer", "billing_writer"];
/// let one = non_empty::NonEmpty::one("app_writer");
/// let configured = non_empty::NonEmpty::try_from(vec!["app_writer"])?;
///
/// assert_eq!(roles.first(), &"app_writer");
/// assert_eq!(roles.len().get(), 2);
/// assert_eq!(one, configured);
/// assert!(non_empty::NonEmpty::<&str>::try_from(vec![]).is_err());
/// # Ok::<(), non_empty::EmptyError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NonEmpty<T> {
    first: T,
    rest: Vec<T>,
}

impl<T> NonEmpty<T> {
    /// `first` followed by `rest`.
    pub fn new(first: T, rest: impl IntoIterator<Item = T>) -> Self {
        Self {
            first,
            rest: rest.into_iter().collect(),
        }
    }

    /// A list of one item.
    pub fn one(item: T) -> Self {
        Self {
            first: item,
            rest: Vec::new(),
        }
    }

    /// The items of `items`, or [`EmptyError`] if there are none.
    pub fn try_from_iter(items: impl IntoIterator<Item = T>) -> Result<Self, EmptyError> {
        let mut items = items.into_iter();
        let Some(first) = items.next() else {
            return Err(EmptyError);
        };

        Ok(Self::new(first, items))
    }

    /// The first item.
    pub fn first(&self) -> &T {
        &self.first
    }

    /// The items after the first.
    pub fn rest(&self) -> &[T] {
        &self.rest
    }

    /// The number of items, at least 1.
    pub fn len(&self) -> NonZeroUsize {
        NonZeroUsize::MIN.saturating_add(self.rest.len())
    }

    /// Appends an item.
    pub fn push(&mut self, item: T) {
        self.rest.push(item);
    }

    /// The items, in order.
    pub fn iter(&self) -> Iter<'_, T> {
        iter::once(&self.first).chain(self.rest.iter())
    }

    /// Applies `f` to every item, keeping the order.
    pub fn map<U>(self, mut f: impl FnMut(T) -> U) -> NonEmpty<U> {
        NonEmpty {
            first: f(self.first),
            rest: self.rest.into_iter().map(f).collect(),
        }
    }

    /// The items as a `Vec`, which is never empty.
    pub fn into_vec(self) -> Vec<T> {
        let mut items = Vec::with_capacity(self.rest.len().saturating_add(1));
        items.push(self.first);
        items.extend(self.rest);

        items
    }
}

/// An iterator over the items of a [`NonEmpty`], by reference.
pub type Iter<'a, T> = iter::Chain<iter::Once<&'a T>, slice::Iter<'a, T>>;

/// An iterator over the items of a [`NonEmpty`], by value.
pub type IntoIter<T> = iter::Chain<iter::Once<T>, vec::IntoIter<T>>;

impl<T> TryFrom<Vec<T>> for NonEmpty<T> {
    type Error = EmptyError;

    fn try_from(items: Vec<T>) -> Result<Self, Self::Error> {
        Self::try_from_iter(items)
    }
}

impl<T> From<NonEmpty<T>> for Vec<T> {
    fn from(items: NonEmpty<T>) -> Self {
        items.into_vec()
    }
}

impl<T> IntoIterator for NonEmpty<T> {
    type Item = T;
    type IntoIter = IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        iter::once(self.first).chain(self.rest)
    }
}

impl<'a, T> IntoIterator for &'a NonEmpty<T> {
    type Item = &'a T;
    type IntoIter = Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<T> Extend<T> for NonEmpty<T> {
    fn extend<I: IntoIterator<Item = T>>(&mut self, items: I) {
        self.rest.extend(items);
    }
}

/// The list given for a [`NonEmpty`] has no items.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("the list is empty; at least one item is required")]
pub struct EmptyError;

/// A [`NonEmpty`](crate::non_empty::NonEmpty) of the given items, like `vec!`. It needs at least one item,
/// so `non_empty![]` does not compile.
///
/// `use pges_core::non_empty;` imports both this macro and the module.
///
/// ```
/// use pges_core::non_empty;
///
/// let endpoints = non_empty!["http://watch-1:50051", "http://watch-2:50051"];
///
/// assert_eq!(endpoints.len().get(), 2);
/// assert_eq!(non_empty![1].into_vec(), vec![1]);
/// ```
///
/// ```compile_fail
/// let none: pges_core::non_empty::NonEmpty<u8> = pges_core::non_empty![];
/// ```
#[macro_export]
macro_rules! non_empty {
    ($first:expr $(, $rest:expr)* $(,)?) => {
        $crate::non_empty::NonEmpty::new($first, [$($rest),*])
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keeps_the_order() {
        let mut items = NonEmpty::new(1, [2, 3]);
        items.push(4);
        items.extend([5]);

        assert_eq!(items.first(), &1);
        assert_eq!(items.rest(), [2, 3, 4, 5]);
        assert_eq!(items.len().get(), 5);
        assert_eq!(items.iter().copied().collect::<Vec<_>>(), [1, 2, 3, 4, 5]);
        assert_eq!((&items).into_iter().count(), 5);
        assert_eq!(
            items.clone().into_iter().collect::<Vec<_>>(),
            [1, 2, 3, 4, 5]
        );
        assert_eq!(Vec::from(items), [1, 2, 3, 4, 5]);
    }

    #[test]
    fn rejects_empty_lists() {
        assert_eq!(NonEmpty::<u8>::try_from(Vec::new()), Err(EmptyError));
        assert_eq!(NonEmpty::<u8>::try_from_iter([]), Err(EmptyError));
        assert_eq!(NonEmpty::try_from(vec![7]), Ok(NonEmpty::one(7)));
        assert_eq!(NonEmpty::try_from_iter(1..4), Ok(NonEmpty::new(1, [2, 3])));
    }

    #[test]
    fn maps_every_item() {
        let names = NonEmpty::new("a", ["b"]).map(str::to_uppercase);

        assert_eq!(names.into_vec(), ["A", "B"]);
    }

    #[test]
    fn macro_needs_one_item() {
        assert_eq!(crate::non_empty![1], NonEmpty::one(1));
        assert_eq!(crate::non_empty![1, 2, 3,], NonEmpty::new(1, [2, 3]));
    }
}
