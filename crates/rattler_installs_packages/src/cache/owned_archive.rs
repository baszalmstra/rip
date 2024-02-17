use rkyv::{
    de::deserializers::SharedDeserializeMap,
    ser::{
        serializers::{
            AlignedSerializer, AllocScratch, CompositeSerializer, FallbackScratch, HeapScratch,
            SharedSerializeMap,
        },
        Serializer as _,
    },
    validation::validators::DefaultValidator,
    AlignedVec, Archive, CheckBytes, Deserialize, Serialize,
};
use std::{marker::PhantomData, ops::Deref};
use thiserror::Error;

/// A wrapper around [`rkyv::Archived<T>`] that owns the bytes that represent the archived value.
///
/// When using [`rkyv::Archived`] you always borrow the bytes it has been build from. This makes
/// it hard to pass around. This type alleviates that issue by instead storing the bytes and
/// implementing `Deref` to access the archived object.
pub struct OwnedArchive<T> {
    bytes: AlignedVec,
    marker: PhantomData<T>,
}

impl<T: Archive> Deref for OwnedArchive<T> {
    type Target = T::Archived;

    fn deref(&self) -> &Self::Target {
        // SAFETY: this is safe because we validated the bytes upon construction.
        unsafe {
            rkyv::archived_root::<T>(&self.bytes)
        }
    }
}

#[derive(Debug, Error)]
#[error("{message}")]
pub struct CorruptedArchive {
    message: String,
}

#[derive(Debug, Error)]
pub enum ReadArchiveError {
    #[error(transparent)]
    CorruptedArchive(#[from] CorruptedArchive),

    #[error(transparent)]
    IoError(#[from] std::io::Error),
}

impl<T: Archive> OwnedArchive<T>
where
    T::Archived: for<'a> CheckBytes<DefaultValidator<'a>>,
{
    /// Constructs a new OwnedArchive from the serialized bytes of `T`.
    ///
    /// This function checks if the content of the buffer
    pub fn new(bytes: AlignedVec) -> Result<Self, CorruptedArchive> {
        // Validate that the passed in bytes actually represent the type.
        let _ = rkyv::validation::validators::check_archived_root::<T>(&bytes).map_err(|e| {
            CorruptedArchive {
                message: e.to_string(),
            }
        })?;
        Ok(Self {
            bytes,
            marker: PhantomData,
        })
    }

    /// Constructs a new OwnedArchive by reading the serialized bytes of `T` from `reader`.
    pub fn from_reader<R: std::io::Read>(mut reader: R) -> Result<Self, ReadArchiveError> {
        let mut bytes = AlignedVec::with_capacity(1024);
        bytes.extend_from_reader(&mut reader)?;
        Ok(OwnedArchive::new(bytes)?)
    }

    /// Writes the bytes of this OwnedArchive to the writer.
    pub fn write<W: std::io::Write>(this: &OwnedArchive<T>, mut writer: W) -> std::io::Result<()> {
        writer.write_all(&this.bytes)
    }

    /// Returns the byte representation of the OwnedArdhive
    pub fn as_bytes(this: &OwnedArchive<T>) -> &[u8] {
        &this.bytes
    }
}

pub type Serializer<const N: usize> = CompositeSerializer<
    AlignedSerializer<AlignedVec>,
    FallbackScratch<HeapScratch<N>, AllocScratch>,
    SharedSerializeMap,
>;

impl<T: Archive> OwnedArchive<T>
where
    T: Serialize<Serializer<4096>>,
{
    /// Constructs a new OwnedArchive from a non archived version of `T`.
    pub fn from_unarchived(unarchived: &T) -> Self {
        let mut serializer = Serializer::<4096>::default();
        serializer
            .serialize_value(unarchived)
            .expect("serialization should always succeed");
        let bytes = serializer.into_serializer().into_inner();
        Self {
            bytes,
            marker: PhantomData,
        }
    }
}

impl<T: Archive> OwnedArchive<T>
where
    T::Archived: Deserialize<T, SharedDeserializeMap>,
{
    /// Deserializes the byte representation of this OwnedArchive into the non archived version of
    /// T.
    pub fn deserialize(this: &OwnedArchive<T>) -> T {
        // SAFETY: Safe because the contents of the bytes have been checked upfront
        unsafe { rkyv::archived_root::<T>(&this.bytes) }
            .deserialize(&mut SharedDeserializeMap::new())
            .expect("the validity of the bytes has already been checked")
    }
}

impl<T: Archive> From<&T> for OwnedArchive<T>
where
    T::Archived: for<'a> CheckBytes<DefaultValidator<'a>>,
    T: Serialize<Serializer<4096>>,
{
    fn from(value: &T) -> Self {
        OwnedArchive::from_unarchived(value)
    }
}
