import dateutil.parser
import json
import Levenshtein
import lr.catalog
import logging
import random
import re
import os
import unidecode
import itertools

logger = logging.getLogger('to_ascii')

non_ascii_re = re.compile('[^a-zA-Z0-9 /._-]')


class ToAscii(object):

    def __init__(self, replacements):
        self.replacements = replacements
        self.root = []
        self.errors = []

    def normalize_path(self, old):
        normalized_path = unidecode.unidecode(old)
        normalized_path = non_ascii_re.sub('-', normalized_path)
        for original, replacement in self.replacements:
            normalized_path = normalized_path.replace(original, replacement)
        return normalized_path

    def add_rename_child(self, path, new_path):
        if path == new_path:
            return
        # get rid of redundant /
        path = os.path.normpath(path)
        new_path = os.path.normpath(new_path)
        if path.startswith(os.path.sep):
            path = path[len(os.path.sep):]
        if new_path.startswith(os.path.sep):
            new_path = new_path[len(os.path.sep):]

        path_l = path.split(os.path.sep)
        new_path_l = new_path.split(os.path.sep)

        if len(path_l) != len(new_path_l):
            raise ValueError("Cannot split a directory into a subfolder")

        if path_l[0] == '':
            start = 1
        else:
            start = 0

        node = {
            'src': 'root',
            'children': self.root[-1],
        }
        for old, new in zip(path_l, new_path_l):
            child = node['children'].get(
                new,
                {
                    'src': old,
                    'dst': new,
                    'children': {},
                }
            )
            node['children'][new] = child
            node = child
            if child['src'] != old:
                raise ValueError(
                    'Inconsistent renaming, %s is renamed from both %s and %s' % (
                        new,
                        child['src'],
                        old,
                    )
                )

    def add_move_child(self, src, path):
        if path == src:
            return
        # get rid of redundant /
        path = os.path.normpath(path)
        if path.startswith(os.path.sep):
            path = path[len(os.path.sep):]
        path_l = path.split(os.path.sep)
        node = {
            'src': 'root',
            'children': self.root[-1],
        }
        for p in path_l:
            child = node['children'].get(
                p,
                {
                    'src': None,
                    'dst': p,
                    'children': {},
                }
            )
            node['children'][p] = child
            node = child
        node['src'] = src

    def dump_actions(self, path):
        json.dump(
            self.root,
            open(path, 'wb'),
            sort_keys=True,
            indent=4
        )

    def merge_folders(self, catalog):
        self.root.append({})
        for root_folder in catalog.root_folders:
            logger.debug('handling root: %s', root_folder.path)
            self.add_rename_child(root_folder.path, root_folder.path)

            def parse_date(path):
                path = os.path.basename(os.path.normpath(path))
                try:
                    return dateutil.parser.parse(path, fuzzy_with_tokens=True)
                except:
                    return None

            def preprocess_folder(folder):
                path = folder.path
                n_path = os.path.normpath(path)
                base_name = os.path.basename(n_path)
                dir_name = os.path.dirname(n_path)
                try:
                    date = dateutil.parser.parse(
                        base_name, fuzzy_with_tokens=True)[0]
                    date = date.replace(tzinfo=None)
                except:
                    date = None
                return (path, dir_name, base_name, date, folder)

            folder_paths = [
                preprocess_folder(folder)
                for folder in root_folder.folders
            ]
            candidates = []

            for path, dir_name, base_name, date, folder in folder_paths:
                if not path:
                    continue

                def folder_match(f):
                    c_path, c_dir_name, c_base_name, c_date, c_folder = f
                    if not c_path:
                        return False
                    if not c_base_name:
                        return False
                    if path == c_path:
                        return False
                    if date != c_date:
                        return False
                    if dir_name != c_dir_name:
                        return False
                    if re.match('^%s-[0-9]+$' % path[:-1], c_path[:-1]):
                        return True
                    return False
                    distance = Levenshtein.jaro(base_name, c_base_name)
                    return distance > 0.98

                if folder not in itertools.chain(*candidates):
                    similar = filter(folder_match, folder_paths)
                    if similar:
                        candidates.append(
                            ([folder] + [_folder for _, _, _, _, _folder in similar])
                        )
            if candidates:
                for candidate in candidates:
                    dst_folder = candidate[0]
                    for src_folder in candidate[1:]:
                        for file in src_folder.files:
                            file_paths = file.paths
                            orig = file.path
                            base, ext = os.path.splitext(orig)
                            suffix = ''
                            for i in range(10):
                                try:
                                    file.folder = dst_folder.id_local
                                except:
                                    suffix = '-%s' % (int(random.random() * 10),)
                                    file.path = '%s%s%s' % (base, suffix, ext)
                                else:
                                    break
                            else:
                                file.path = orig
                            for path in file_paths:
                                ext = os.path.splitext(path)[1]
                                self.add_move_child(
                                    os.path.join(root_folder.path,
                                                 src_folder.path, path),
                                    os.path.join(
                                        root_folder.path, dst_folder.path, '%s%s%s' % (base, suffix, ext))
                                )
        catalog.conn.commit()

    def to_ascii(self, catalog):
        self.root.append({})
        for root_folder in catalog.root_folders:
            logger.debug('handling root: %s', root_folder.path)
            self.add_rename_child(root_folder.path, root_folder.path)

            for folder in root_folder.folders:
                logger.debug('managing folder %s', folder.path)

                src_folder = os.path.normpath(folder.path)
                if src_folder == '.':
                    continue
                dst_folder = os.path.normpath(self.normalize_path(src_folder))
                if src_folder != dst_folder:
                    suffix = ''
                    # In case lightroom stored the same folder twice with different encoding
                    # (mounting over samba/afp/nfs), normalization lead to unique exception violation.
                    # work-around: rename the destination folder for future
                    # manual / automatic merge
                    for retry in xrange(10):
                        try:
                            folder.path = dst_folder + suffix
                            pass
                        except lr.catalog.DuplicateDirectory as e:
                            suffix = '-%d' % (retry + 1,)
                        else:
                            self.add_rename_child(
                                os.path.join(root_folder.path, src_folder),
                                os.path.join(root_folder.path, dst_folder)
                            )
                            break
                    else:
                        self.logger.warn(
                            'Failed to rename folder %s to %s, error: %s',
                            src_folder,
                            dst_folder,
                            e
                        )
                        self.errors.append([src_folder, dst_folder])
                        continue
                for file in folder.files:
                    for path in file.paths:
                        normalized_path = self.normalize_path(path)
                        for i in range(10):
                            try:
                                file.path = normalized_path
                            except:
                                base, ext = os.path.splitext(file.path)
                                normalized_path = '%s-%s%s' % (
                                    base, int(random.random() * 10), ext)
                            else:
                                break
                        self.add_rename_child(
                            os.path.join(root_folder.path, src_folder, path),
                            os.path.join(root_folder.path,
                                         dst_folder, normalized_path)
                        )
        catalog.conn.commit()
