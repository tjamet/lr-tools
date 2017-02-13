import sqlite3
import logging
import os


class DuplicateDirectory(Exception):
    pass


class LRTable(object):

    def __init__(self, catalog, id_local):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.id_local = id_local
        self.catalog = catalog


class File(LRTable):

    @property
    def folder(self):
        c = self.catalog.conn.cursor()
        c.execute(
            'SELECT folder FROM AgLibraryFile WHERE id_local = ?', (self.id_local,))
        return c.fetchone()[0]

    @folder.setter
    def folder(self, value):
        self.catalog.write('''
                  UPDATE
                        AgLibraryFile
                     SET
                        folder = :folder
                   WHERE
                        id_local = :id_local
            ''',
                           {
                               'folder': value,
                               'id_local': self.id_local,
                           }
                           )

    @property
    def path(self):
        c = self.catalog.conn.cursor()
        c.execute('''
                  SELECT
                        baseName || '.' || extension
                    FROM
                        AgLibraryFile
                   WHERE
                        AgLibraryFile.id_local = ?

            ''',
                  (self.id_local,)
                  )
        return c.fetchone()[0]

    @path.setter
    def path(self, value):
        value = os.path.splitext(value)[0]
        self.catalog.write('''
                  UPDATE
                        AgLibraryFile
                     SET
                        baseName = :baseName,
                        idx_filename = :baseName || '.' || extension,
                        lc_idx_filename = LOWER(:baseName || '.' || extension)
                   WHERE
                        AgLibraryFile.id_local = :id_local
            ''',
                           {
                               'id_local': self.id_local,
                               'baseName': value,
                           }
                           )

    @property
    def paths(self):
        c = self.catalog.conn.cursor()
        c.execute('''
                  SELECT
                        baseName,
                        extension,
                        sidecarExtensions
                    FROM
                        AgLibraryFile
                   WHERE
                        AgLibraryFile.id_local = ?

            ''',
                  (self.id_local,)
                  )
        baseName, extension, sidecarExtensions = c.fetchone()
        return [
            '%s.%s' % (baseName, extension)
            for ext in [extension] + sidecarExtensions.split(',')
        ]


class Folder(LRTable):

    @property
    def root(self):
        return RootFolder(self.catalog, self.root_id)

    @property
    def root_id(self):
        c = self.catalog.conn.cursor()
        c.execute('''
              SELECT
                    rootFolder
                FROM
                    AgLibraryFolder
               WHERE
                    AgLibraryFolder.id_local = ?;
        ''', (self.id_local,))
        return c.fetchone()[0]

    @property
    def files(self):
        c = self.catalog.conn.cursor()
        c.execute('''
              SELECT
                    AgLibraryFile.id_local
                FROM
                    AgLibraryFile
               WHERE
                    folder = ?
            ''',
                  (self.id_local,)
                  )
        return [
            File(self.catalog, f)
            for f, in c
        ]

    @property
    def path(self):
        c = self.catalog.conn.cursor()
        c.execute('''
              SELECT
                    pathFromRoot
                FROM
                    AgLibraryFolder
               WHERE
                    AgLibraryFolder.id_local = ?;
        ''', (self.id_local,))
        return c.fetchone()[0]

    def __cmp__(self, other):
        return cmp(self.path, other.path)

    @path.setter
    def path(self, value):
        if not value.endswith(os.path.sep):
            value = '%s%s' % (value, os.path.sep)
        if value in ('./', '/'):
            value = ''
        try:
            self.catalog.write(
                '''
                      UPDATE
                            AgLibraryFolder
                         SET
                            pathFromRoot= :new
                       WHERE
                            id_local = :id
                ''',
                {
                    'id': self.id_local,
                    'new': value,
                }
            )
        except sqlite3.IntegrityError as e:
            raise DuplicateDirectory(str(e))


class RootFolder(LRTable):

    @property
    def path(self):
        c = self.catalog.conn.cursor()
        c.execute('''
              SELECT
                    absolutePath
                FROM
                    AgLibraryRootFolder
               WHERE
                    id_local = ?;
        ''', (self.id_local,))
        return c.fetchone()[0]

    @property
    def folders(self):
        c = self.catalog.conn.cursor()
        c.execute('''
              SELECT
                    AgLibraryFolder.id_local
                FROM
                    AgLibraryFolder
               WHERE
                    rootFolder = ?
            ''',
                  (self.id_local,)
                  )
        return [
            Folder(self.catalog, id_local)
            for id_local, in c
        ]

    @path.setter
    def path(self, value):
        self.catalog.write('''
              UPDATE
                    AgLibraryRootFolder
                 SET
                    absolutePath = ?
               WHERE
                    id_local = ?;
        ''', (value, self.id_local,))
        c.connection.commit()


class Catalog(object):

    def __init__(self, db):
        self.db = db
        self.conn = sqlite3.connect(db)
        self.logger = logging.getLogger(self.__class__.__name__)

    def write(self, cmd, args):
        c = self.conn.cursor()
        c.execute(cmd, args)

    @property
    def root_folders(self):
        c = self.conn.cursor()
        c.execute('''
              SELECT
                    id_local
                FROM
                    AgLibraryRootFolder
        ''')
        return [
            RootFolder(self, id_local)
            for id_local, in c
        ]

    @property
    def folders(self):
        c = self.conn.cursor()
        c.execute('''
              SELECT
                    AgLibraryFolder.id_local
                FROM
                    AgLibraryFolder
        ''')
        return [
            Folder(self, id_local)
            for id_local, in c
        ]

    def get_folder_by_rel_path(self, root_id, rel_path):
        c = self.conn.cursor()
        c.execute('''
              SELECT
                    AgLibraryFolder.id_local
                FROM
                    AgLibraryFolder
               WHERE
                    pathFromRoot = ?
                 AND
                    rootFolder = ?
            ''',
                  (rel_path, root_id,)
                  )
        return Folder(self, c.fetchone()[0])

    def files_in_folder(self, folder_id):
        c = self.conn.cursor()
        c.execute('''
              SELECT
                    AgLibraryFile.id_local
                FROM
                    AgLibraryFile
               WHERE
                    folder = ?
            ''',
                  (folder_id,)
                  )
        return [
            File(self, f)
            for f, in c
        ]

    def get_folders_matching(self, path):
        c = self.conn.cursor()
        s.execute('''
                  SELECT
                        id_local
                    FROM
                        AgLibraryFolder
                   WHERE
                        pathFromRoot LIKE ?
            ''',
                  (path,)
                  )
        for i, in c:
            yield Folder(self, i)
