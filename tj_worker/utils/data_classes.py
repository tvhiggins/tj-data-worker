from dataclasses import dataclass, field


@dataclass
class FileItem:
    """Represents a local file.

    Args:
        file_name (int): File name
        full_local_path (int): Full local path
    """

    file_name: str
    full_local_path: str
    headers: list = field(default_factory=list)


@dataclass
class ListofFiles:
    _items: list[FileItem] = field(default_factory=list)

    def addFile(self, file: FileItem):
        if not isinstance(file, FileItem):
            e = ValueError()
            raise e
        # sure this shouldn't be "self._package.append(param)"?
        self._items.append(file)

