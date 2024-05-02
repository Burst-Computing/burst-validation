import bz2
from pprint import pprint
from time import time

import click
import joblib
from lithops import FunctionExecutor, Storage
from sklearn.feature_extraction.text import HashingVectorizer
from sklearn.linear_model import SGDClassifier
from sklearn.pipeline import Pipeline


def load_data(dataset, mib):
    # Download the dataset at
    # https://www.kaggle.com/bittlingmayer/amazonreviews

    print("Loading Amazon reviews dataset:")
    compressed = bz2.BZ2File(dataset)

    X = []
    y = []
    total_size = 0
    for _ in range(3_600_000):
        line = compressed.readline().decode('utf-8')
        X.append(line[11:])
        y.append(int(line[9]) - 1)  # __label__1, __label__2

        total_size += len(line[11:])
        if (total_size / 2 ** 20) > mib:
            break

    print("\t%d reviews" % len(X))
    print("\t%0.2f MiB of data" % (total_size / 2 ** 20))
    return X, y



def func(backend='loky', mib=10.0, refit=False, jobs=-1, dataset='./train.ft.txt.bz2'):
    X, y = load_data(dataset, mib)

    n_features = 2 ** 18
    pipeline = Pipeline([
        ('vect', HashingVectorizer(n_features=n_features, alternate_sign=False)),
        ('clf', SGDClassifier()),
    ])

    parameters = {
        'vect__norm': ('l1', 'l2'),
        'vect__ngram_range': ((1, 1), (1, 2)),
        'clf__alpha': (1e-2, 1e-3, 1e-4, 1e-5),
        'clf__max_iter': (10, 30, 50, 80),
        'clf__penalty': ('l2', 'l1', 'elasticnet')
    }

    # loky
    from sklearn.model_selection import GridSearchCV
    grid_search = GridSearchCV(pipeline, parameters,
        error_score='raise', refit=refit, cv=5, n_jobs=jobs)

    print("pipeline:", [name for name, _ in pipeline.steps])
    print("parameters: ", end='')
    pprint(parameters)

    with joblib.parallel_config(backend=backend, n_jobs=jobs):
        print("Performing grid search...")
        t0 = time()
        grid_search.fit(X, y)
        total_time = time() - t0
        print("Done in %0.3fs\n" % total_time)

    if refit:
        print("Best score: %0.3f" % grid_search.best_score_)
        print("Best parameters set:")
        best_parameters = grid_search.best_estimator_.get_params()
        for param_name in sorted(parameters.keys()):
            print("\t%s: %r" % (param_name, best_parameters[param_name]))


def map_func(id, storage: Storage, mib: float, bucket: str, key: str):
    print(f"Running job {id}")
    dataset = storage.get_object(bucket, key)
    print(f"Downloaded {len(dataset) / 2**20} MiB")
    with open(f'/tmp/{id}.txt.bz2', 'wb') as f:
        f.write(dataset)
    print(f"Saved to /tmp/{id}.txt.bz2")
    func(mib=mib, dataset=f'/tmp/{id}.txt.bz2', jobs=1)


@click.command()
@click.option('--total-mib', default=20, help='Total MiB of data to load')
@click.option('--workers', default=1, help='Number of workers to use')
@click.option('--bucket', default='hypertuning', help='Bucket name')
@click.option('--key', default='train.ft.txt.bz2', help='Key name')

def main(total_mib: float, workers: int, bucket: str, key: str):
    fexec = FunctionExecutor()
    mib_per_worker = total_mib / workers
    iterdata = [(mib_per_worker, bucket, key) for i in range(workers)]
    fexec.map(map_func, iterdata)
    fexec.wait()
    fexec.plot()


if __name__ == "__main__":
    main()
