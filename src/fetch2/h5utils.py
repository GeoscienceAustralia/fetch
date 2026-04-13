"""
Utility functions used in the conversion process to hdf5 archives
"""

from typing import Optional, List, Dict
from io import StringIO, BufferedReader
import os
import uuid
import urllib.parse
import hashlib
from pathlib import Path
import tempfile
from contextlib import contextmanager

try:
    from ruamel.yaml import YAML as _YAML
    import h5py
except ImportError:  # pragma: no cover
    _YAML = None
    h5py = None


FALLBACK_UUID_NAMESPACE = uuid.UUID("c5908e58-7301-4054-9f04-a0fa8cdef63b")
PUBLIC_NAMESPACE = "METADATA"
PRIVATE_NAMESPACE = ".METADATA"
METADATA_PTR = "CURRENT"
METADATA_LIST_PTR = "CURRENT-LIST"

YAML = _YAML() if _YAML is not None else None
VLEN_STRING = h5py.special_dtype(vlen=str) if h5py is not None else None


def _require_runtime_deps():
    missing = []
    if _YAML is None:
        missing.append("ruamel.yaml")
    if h5py is None:
        missing.append("h5py")

    if missing:
        deps = ", ".join(missing)
        raise RuntimeError(
            "Water vapour dependencies are not installed "
            f"({deps}). Install the feature extra with "
            "`uv sync --extra water-vapour` or "
            "`uv pip install -e '.[water-vapour]'`."
        )


def _get_next_md_id(h5_group: h5py.Group, group_prefix: str) -> int:
    """
    Incrementer used to name internal metadata document references

    :param h5_group:
        h5File root Group
    :param group_prefix:
        group_prefix for the metadata documents

    :return:
        next numeric id to use as a document name
    """
    _require_runtime_deps()
    ids = [0]

    def _append_id(h5_key):
        ids.append(int(h5_key))

    group = h5_group.get(group_prefix)
    if group:
        group.visit(_append_id)

    return max(ids) + 1


def _write_dataset(
    h5_group: h5py.Group,
    dataset: Dict,
    dataset_path: str = "",
    track_order: bool = True,
) -> h5py.Dataset:
    """
    Internal function for writing dataset metadata to a H5file

    :param h5_group:
        Root reference for the h5 collection
    :param dataset:
        metadata encoded as a nested dictionary
    :param dataset_path:
        dataset path corresponding to the metadata
    :param track_order:
        flag to track insertion order on h5Groups
    """
    _require_runtime_deps()
    dataset_path = dataset_path.lstrip("/")
    doc_group = "/".join((PRIVATE_NAMESPACE, dataset_path))
    if not h5_group.get(doc_group):
        create_groups(h5_group, doc_group, track_order=track_order)

    doc_id = str(_get_next_md_id(h5_group, doc_group))
    ds = h5_group.create_dataset(
        "/".join((PRIVATE_NAMESPACE, dataset_path, doc_id)),
        dtype=VLEN_STRING,
        shape=(1,),
    )

    with StringIO() as stream:
        YAML.dump(dataset, stream)
        ds[()] = stream.getvalue()

    public_group = "/".join((PUBLIC_NAMESPACE, dataset_path))
    public_path = public_group + "/" + METADATA_PTR

    if not h5_group.get(public_group):
        h5_group.create_group(public_group, track_order=track_order)
    if h5_group.get(public_path):
        del h5_group[public_path]

    h5_group[public_path] = h5py.SoftLink(ds.name)

    return h5_group[public_path]


def create_groups(root: h5py.Group, group_path: str, track_order: bool = True):
    """
    Create hdf5.Group chain ensuring that the track order parameter is set at
    each level if the group is absent

    :param root:
        root of the h5py collection
    :param group_path:
        group path to make parents for
    :param track_order:
        determines if the order for the h5py.Groups should be tracked
    """
    _require_runtime_deps()
    _parts = Path(group_path).parts

    if root.get(group_path):
        return  # early exit

    for _idx in range(1, len(_parts)):
        _path = os.path.join(*_parts[:_idx])
        if not root.get(os.path.join(*_parts[:_idx])):
            root.create_group(_path, track_order=track_order)


def _append_data_to_existing_file(
    inh5: h5py.Group, outh5: h5py.Group, track_order: bool = True
):
    """
    Internal function to handle appending new data to an existing h5File

    :param inh5:
        Group to read from
    :param outh5:
        Group to write to
    :param track_order:
        Add insertion order tracking to created groups
    """

    _require_runtime_deps()
    if track_order:
        _traversal_step = -1
    else:
        _traversal_step = 1

    def _traverse(root: h5py.Group, offset: str):
        if isinstance(root[offset], h5py.Dataset):
            yield root[offset].name
        elif isinstance(root[offset], h5py.Group):
            for k in list(root[offset].keys())[::_traversal_step]:
                if isinstance(root[os.path.join(offset, k)], h5py.Dataset):
                    yield root[os.path.join(offset, k)].name
                else:
                    for name in _traverse(root, os.path.join(offset, k)):
                        yield name

    md_docs = []
    md_names = []

    # Confirm new non-metadata datasets are missing in output file
    for k in list(inh5.keys()):
        if k in (PUBLIC_NAMESPACE, PRIVATE_NAMESPACE):
            continue
        else:
            for ds_path in _traverse(inh5, k):
                if outh5.get(ds_path):
                    raise RuntimeError(
                        "Dataset {} already exists in file: {}".format(
                            ds_path, outh5.filename
                        )
                    )
    for k in list(inh5.keys())[::_traversal_step]:
        if k == PUBLIC_NAMESPACE:
            continue
        elif k == PRIVATE_NAMESPACE:
            for _md in _traverse(inh5, k):
                md_docs.append(YAML.load(inh5[_md][()].item()))
                md_names.append("/".join(_md.split("/")[2:-1]))
        else:
            for ds_path in _traverse(inh5, k):
                if track_order:
                    create_groups(
                        outh5, ds_path.rsplit("/", 1)[0], track_order=track_order
                    )
                inh5.copy(ds_path, outh5, name=ds_path)

    write_h5_md(outh5, md_docs, md_names)


def write_h5_md(
    h5_group: h5py.Group,
    datasets: List[Dict],
    dataset_names: Optional[List[str]] = None,
    track_order=True,
) -> None:
    """
    Appends metadata documents to a h5File collection, updating
    SoftLinks in the public namespace and the metadata listing in the
    public namespace.

    :param h5_group:
        reference to the root h5py.Group to write to
    :param datasets:
        An array of metadata documents encoded as dictionaries
    :param dataset_names:
        An array of dataset names corresponding to each of the documents;
        for a single dataset h5 collection provide '/'
    """

    _require_runtime_deps()
    collection_path = "/".join((PUBLIC_NAMESPACE, METADATA_LIST_PTR))
    known_metadata_refs = []
    new_metadata_refs = []

    if h5_group.get(collection_path):
        # Collate known metadata references; requires access the h5py internal methods
        existing_refs = h5_group[collection_path]
        for i in range(existing_refs._dcpl.get_virtual_count()):  # pylint: disable=protected-access
            known_metadata_refs.append(existing_refs._dcpl.get_virtual_dsetname(i))  # pylint: disable=protected-access
        existing_refs = None

    # Create metadata and collate new references
    for i, _ in enumerate(datasets):
        curr_ref = _write_dataset(
            h5_group, datasets[i], dataset_names[i], track_order=track_order
        )
        if curr_ref.name not in known_metadata_refs:
            new_metadata_refs.append(curr_ref.name)

    if new_metadata_refs:
        # Extend the virtual layout for new datasets
        virtual_collection = h5py.VirtualLayout(
            shape=(len(known_metadata_refs) + len(new_metadata_refs),),
            dtype=VLEN_STRING,
        )

        ds_cntr = 0
        for ds_cntr, _ in enumerate(known_metadata_refs):
            virtual_collection[ds_cntr] = h5py.VirtualSource(
                path_or_dataset=".",
                name=known_metadata_refs[ds_cntr],
                dtype=VLEN_STRING,
                shape=(1,),
            )

        # Increment counter if a dataset was written
        if known_metadata_refs:
            ds_cntr = ds_cntr + 1

        for j, _ in enumerate(new_metadata_refs):
            virtual_collection[ds_cntr + j] = h5py.VirtualSource(
                path_or_dataset=".",
                name=new_metadata_refs[j],
                dtype=VLEN_STRING,
                shape=(1,),
            )

        # Recreate the virtual collection
        if h5_group.get(collection_path):
            del h5_group[collection_path]

        h5_group.create_virtual_dataset(collection_path, virtual_collection)


def generate_fallback_uuid(
    product_href: str,
    uuid_namespace: uuid.UUID = FALLBACK_UUID_NAMESPACE,
    **product_params,
):
    """
    Generates a fallback UUID from the DEA product_href and fallback UUID namespace
    :param product_href:
        href to identify the product the dataset relates to
    :param uuid_namespace:
        A namespace used to generate a uuid
    :param product_params:
        A dictionary urlencoded and appened to the product_href for deterministic uuids

    :return:
        UUID for the specified product
    """
    _require_runtime_deps()
    return uuid.uuid5(
        uuid_namespace,
        "{}?{}".format(product_href, urllib.parse.urlencode(product_params)),
    )


def generate_md5sum(src: BufferedReader, chunk_size: int = 16384):
    """
    Generate a md5sum for the src component.
    Used to help generate fallback uuids
    :param src:
        a buffered reader used to calculate md5sum
    :param chunk_size:
        chunk_size to used to calculate md5sum
    """
    _require_runtime_deps()
    md5_hash = hashlib.md5()
    for chunk in iter(lambda: src.read(chunk_size), b""):
        md5_hash.update(chunk)

    return md5_hash


@contextmanager
def atomic_h5_write(fname: Path, mode: str = "a", **kwargs):
    """
    Will create a temporary h5File location to validate dataset
    conversion. After completion it will move the file if write is
    specified or no file exists; otherwise it will append the new datasets
    and metadata to the specified location.
    In the event of an error the temporary location will be deleted

    :param fname:
        path to final location for the dataset
    :param mode:
        mode in which to open the file; one of 'a', 'w'
    :param kwargs:
        key word arguments to h5file creation

    """
    _require_runtime_deps()
    os_fid, tpath = tempfile.mkstemp(dir=fname.parent, prefix=".tmp", suffix=".h5")
    fp = Path(tpath)
    preexisting = fname.exists()
    # Fix, don't break read mode
    try:
        with h5py.File(tpath, mode=mode, **kwargs) as h5_ref:
            yield h5_ref

            if mode == "a" and preexisting:
                with h5py.File(fname, mode="a", **kwargs) as _out:
                    # Append datasets to file and delete temporary file
                    _append_data_to_existing_file(
                        h5_ref, _out, track_order=kwargs.get("track_order", True)
                    )
        if mode == "w" or (mode == "a" and not preexisting):
            fp.rename(fname)
            fp = None
        else:
            fp.unlink()
            fp = None
    finally:
        os.close(os_fid)
        if fp and fp.exists():
            fp.unlink()
