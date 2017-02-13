import os
import shutil
import sys
import json


def mkdir_p(p):
    if not os.path.exists(p):
        os.makedirs(p)


def replace(path, replacements):
    for orig, new in replacements.items():
        if path.startswith(orig):
            path = new + path[len(orig):]
    return path


def apply_node(parent, node, replacements):
    src = node.get('src', None)
    dst = node['dst']
    dst_path = os.path.join(parent, dst)
    if src is not None:
        src_path = os.path.join(parent, src)
        if src_path != dst_path:
            src_path = replace(src_path, replacements)
            dst_path = replace(dst_path, replacements)
            if os.path.exists(src_path):
                mkdir_p(os.path.dirname(dst_path))
                shutil.move(src_path, dst_path)
            else:
                print 'Cannot move %s to %s, source path does not exist' % (src_path, dst_path)
    [apply_node(dst_path, _, replacements) for _ in node['children'].values()]


def apply_renames(renames, replacements):
    for k, v in renames.items():
        apply_node('/', v, replacements)


def main(argv=None):
    if argv is None:
        argv = sys.argv
    path = argv[-1]
    replacements = dict([
        arg.split(':')
        for arg in argv[1:-1]
    ])
    print replacements
    renames = json.load(open(argv[-1]))
    for rename in renames:
        apply_renames(rename, replacements)


if __name__ == '__main__':
    main()
