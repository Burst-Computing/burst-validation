from sklearn.feature_extraction.text import HashingVectorizer
from sklearn.linear_model import SGDClassifier
from sklearn.pipeline import Pipeline

import joblib

from pprint import pprint
from time import time
import click
import bz2


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


@click.command()
@click.option('--backend', default='loky', help='Joblib backend to perform grid search '
                                                '(loky | lithops | dask | ray | tune)')
@click.option('--address', default=None, help='Scheduler address (dask) or head node address '
                                              '(ray, ray[tune])')
@click.option('--mib', default=10, type=float, help='Load X MiB from the dataset')
@click.option('--refit', default=False, is_flag=True, help='Fit the final model with the best '
                                                           'configuration and print score')
@click.option('--jobs', default=-1, help='Number of jobs to execute the search. -1 means all processors.')
@click.option('--dataset', default='./train.ft.txt.bz2', help='Amazon reviews dataset file')

def main(backend, address, mib, refit, jobs, dataset):

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


if __name__ == "__main__":
    main()
