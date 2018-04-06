import argparse
from collections import defaultdict
import dataset
import json
import os
import random

DEFAULT_URL = 'dataset.sqlite'

def get_database(url):
    url = url or DEFAULT_URL
    if '://' not in url:
        url = "sqlite:///{}".format(url)
    db0 = dataset.connect(url)
    with db0 as db:
        labels = db["labels"]
        labels.create_column_by_example('name', 'string')
        splits = db["splits"]
        splits.create_column_by_example('name', 'string')
        samples_labels = db["samples_labels"]
        samples_labels.create_column_by_example('name', 'string')
        samples_labels.create_column_by_example('filename', 'string')
        samples_labels.create_index(['filename'])
        samples_splits = db["samples_splits"]
        samples_splits.create_column_by_example('name', 'string')
        samples_splits.create_column_by_example('filename', 'string')
        samples_splits.create_index(['filename'])
    return db0

def show_stats(db):
    stats = db.query("select"
                     "   samples_labels.name as label,"
                     "   samples_splits.name as split,"
                     "   count(*) as count"
                     " from samples_labels"
                     " left join samples_splits"
                     "   on samples_labels.filename = samples_splits.filename"
                     " group by samples_labels.name, samples_splits.name"
                     " order by samples_labels.name, samples_splits.name")
    for stat in stats:
        print("{} {}: {} sample(s)".format(stat["label"], stat["split"] or "(no split)", stat["count"]))
        
def cmd_add(args):
    db = get_database(args.db)
    labels = db["labels"]
    splits = db["splits"]
    label = labels.find_one(name=args.name)
    split = splits.find_one(name=args.name)
    if args.split:
        label = None
    if args.label:
        split = None
    if label and split:
        print("{} is ambiguous, is it a label or a split?".format(args.name))
        exit(1)
    if not (label or split):
        if args.split:
            split = splits.insert({'name': args.name})
        elif args.label:
            label = labels.insert({'name': args.name})
        else:
            print("{} is an unknown label/split, please add it first".format(args.name))
            exit(1)
    table = "samples_labels" if label else "samples_splits"
    with db as tx:
        for filename in args.files:
            if not os.path.isfile(filename):
                print("{} is not a file, skipping".format(filename))
                continue
            prev = tx[table].find_one(filename=filename)
            if prev:
                if prev["name"] == args.name:
                    continue
                print("Saw {} previously as {} - correcting".format(filename, prev["name"]))
                tx[table].delete(filename=filename)
            tx[table].insert({'name': args.name, 'filename': filename})
    show_stats(db)

def cmd_remove(args):
    db = get_database(args.db) 
    with db as tx:
        for table in ["samples_labels", "samples_splits"]:
            for filename in args.files:
                tx["samples"].delete(filename=filename)
    show_stats(db)

def cmd_part(table, args):
    db = get_database(args.db)
    with db as tx:
        for name in args.names:
            if not args.remove:
                tx[table].upsert({'name': name}, ['name'])
            else:
                tx[table].delete(name=name)
    print("{}:".format(table), [row["name"] for row in db[table].all(order_by="name")])

def get_list(db):
    splits = {}
    result = {
        "splits": splits
    }
    labels = [row["name"] for row in db["labels"].all(order_by='name')]
    for split in [row["name"] for row in db["splits"].all()] + [None]:
        samples = db.query("select samples_labels.filename, samples_labels.name from samples_labels left join samples_splits on samples_labels.filename = samples_splits.filename where samples_splits.name is :split order by samples_labels.name, samples_splits.filename", split=split)
        out = defaultdict(list)
        for s in samples:
            out[s["name"]].append(s["filename"])
        order = [out[label] for label in labels]
        data = splits[split] = {
            "split": split,
            "labels": labels,
            "samples": order
        }
    return result
    
def cmd_list(args):
    db = get_database(args.db)
    if args.json:
        print(json.dumps(get_list(db), indent=2))
    else:
        stats = db.query("select"
                         "   samples_labels.name as label,"
                         "   samples_splits.name as split,"
                         "   samples_labels.filename"
                         " from samples_labels"
                         " left join samples_splits"
                         "   on samples_labels.filename = samples_splits.filename"
                         " order by samples_labels.name, samples_splits.name")
        for stat in stats:
            print("{},{},{}".format(stat["label"], stat["split"] or "null", stat["filename"]))

def cmd_move(args):
    db = get_database(args.db)
    src = args.source_split or None
    dest = args.dest_split or None
    percent = args.percentage
    splits = db["splits"]
    if src and not splits.find_one(name=src):
        print("Do not know split {}, please add it first".format(src))
        exit(1)
    if dest and not splits.find_one(name=dest):
        print("Do not know split {}, please add it first".format(dest))
        exit(1)
    with db as tx:
        samples = db.query("select samples_labels.filename, samples_labels.name from samples_labels left join samples_splits on samples_labels.filename = samples_splits.filename where samples_splits.name is :split order by samples_labels.name, samples_splits.filename", split=src)
        # samples = list(db["samples_splits"].find(name=src))
        out = defaultdict(list)
        for s in samples:
            out[s["name"]].append(s["filename"])
        for grp in out.values():
            random.shuffle(grp)
            grp = grp[0:round(len(grp) * percent / 100.0)]
            for sample in grp:
                tx["samples_splits"].update({'name': dest, 'filename': sample}, ['filename'])
    show_stats(db)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--db', type=str, default=DEFAULT_URL,
                        help='database file to store sample list in')

    subparsers = parser.add_subparsers(help='sub-command help')

    parser_labels = subparsers.add_parser('labels', help='control labels')
    parser_labels.add_argument('names', nargs='+',
                                help='labels to add')
    parser_labels.add_argument('--remove', action='store_true',
                                help='remove rather than adding labels')
    parser_labels.set_defaults(func=lambda args: cmd_part('labels', args))

    parser_splits = subparsers.add_parser('splits', help='control splits')
    parser_splits.add_argument('names', nargs='+',
                                help='splits to add')
    parser_splits.add_argument('--remove', action='store_true',
                                help='remove rather than adding splits')
    parser_splits.set_defaults(func=lambda args: cmd_part('splits', args))

    parser_add = subparsers.add_parser('add', help='add samples')
    parser_add.add_argument('name',
                            help='label/split for files we are adding')
    parser_add.add_argument('files', nargs='*',
                            help='samples to add')
    parser_add.add_argument('--label', action='store_true',
                            help='add label name')
    parser_add.add_argument('--split', action='store_true',
                            help='add split name')
    # parser.add_argument('--modulo', type=int, default=1,
    #                  help='which samples to use, every 1 in N of list')
    # parser.add_argument('--include', type=int, default=1,
    # help='which samples to use, every 1 in N of list')
    parser_add.set_defaults(func=cmd_add)

    parser_break = subparsers.add_parser('move', help='move samples between splits')
    parser_break.add_argument('source_split', type=str,
                            help='split to take samples from')
    parser_break.add_argument('dest_split', type=str,
                            help='split to move samples to')
    parser_break.add_argument('percentage', type=float,
                            help='how many samples to move (as a percentage)')
    parser_break.set_defaults(func=cmd_move)
    
    parser_remove = subparsers.add_parser('remove', help='remove samples')
    parser_remove.add_argument('files', nargs='*',
                                help='samples to remove')
    parser_remove.set_defaults(func=cmd_remove)

    parser_list = subparsers.add_parser('list', help='list samples')
    parser_list.add_argument('--json', action='store_true',
                             help='list in json format')
    parser_list.set_defaults(func=cmd_list)

    args = parser.parse_args()
    if 'func' in args:
        args.func(args)
    else:
        parser.print_help()
        exit(1)

if __file__ == '__main__':
    main()
