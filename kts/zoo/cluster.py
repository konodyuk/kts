from sklearn.cluster import KMeans
import numpy as np


try:
    from libKMCUDA import kmeans_cuda
except:
    pass


class KMeansFeaturizer:
    """Transforms numeric data into k-means cluster memberships.

    This transformer runs k-means on the input data and converts each data point
    into the id of the closest cluster. If a target variable is present, it is
    scaled and included as input to k-means in order to derive clusters that
    obey the classification boundary as well as group similar points together.

    Parameters
    ----------
    k: integer, optional, default 100
        The number of clusters to group data into.

    target_scale: float, [0, infty], optional, default 5.0
        The scaling factor for the target variable. Set this to zero to ignore
        the target. For classification problems, larger `target_scale` values
        will produce clusters that better respect the class boundary.

    random_state : integer or numpy.RandomState, optional
        This is passed to k-means as the generator used to initialize the
        kmeans centers. If an integer is given, it fixes the seed. Defaults to
        the global numpy random number generator.

    Attributes
    ----------
    cluster_centers_ : array, [k, n_features]
        Coordinates of cluster centers. n_features does count the target column.

    References
    ----------
    This implementation is partially taken from:
        "Feature Engineering for Machine Learning by Alice Zheng and Amanda Casari (O’Reilly).
         Copyright 2018 Alice Zheng and Amanda Casari, 978-1-491-95324-2."
    """

    def __init__(self, k=100, target_scale=5.0, random_state=1337, n_jobs=None):
        self.k = k
        self.target_scale = target_scale
        self.random_state = random_state
        self.n_jobs = n_jobs

    def fit(self, X, y=None):
        """Runs k-means on the input data and find centroids.

        If no target is given (`y` is None) then run vanilla k-means on input
        `X`.

        If target `y` is given, then include the target (weighted by
        `target_scale`) as an extra dimension for k-means clustering. In this
        case, run k-means twice, first with the target, then an extra iteration
        without.

        After fitting, the attribute `cluster_centers_` are set to the k-means
        centroids in the input space represented by `X`.

        Parameters
        ----------
        X : array-like or sparse matrix, shape=(n_data_points, n_features)

        y : vector of length n_data_points, optional, default None
            If provided, will be weighted with `target_scale` and included in
            k-means clustering as hint.
        """
        if y is None:
            # No target variable, just do plain k-means
            try:
                clusters, assignments = kmeans_cuda(X.astype(np.float32),
                                                    clusters=self.k,
                                                    seed=self.random_state
                                                    )
                km_model = KMeans(n_clusters=self.k)
                clusters[~np.isfinite(clusters)] = -1e9
                km_model.cluster_centers_ = clusters
            except:
                km_model = KMeans(n_clusters=self.k,
                                  n_init=20,
                                  random_state=self.random_state,
                                  n_jobs=self.n_jobs
                                  )
                km_model.fit(X)

            self.km_model = km_model
            self.cluster_centers_ = km_model.cluster_centers_
            return self

        # There is target information. Apply appropriate scaling and include
        # into input data to k-means
        data_with_target = np.hstack((X, y[:, np.newaxis] * self.target_scale))

        # Build a pre-training k-means model on data and target
        try:
            clusters, assignments = kmeans_cuda(data_with_target.astype(np.float32),
                                                clusters=self.k,
                                                seed=self.random_state
                                                )
            km_model_pretrain = KMeans(n_clusters=self.k)
            clusters[~np.isfinite(clusters)] = -1e9
            km_model_pretrain.cluster_centers_ = clusters
        except:
            km_model_pretrain = KMeans(n_clusters=self.k,
                                       n_init=20,
                                       random_state=self.random_state,
                                       n_jobs=self.n_jobs
                                       )
            km_model_pretrain.fit(data_with_target)

        # Run k-means a second time to get the clusters in the original space
        # without target info. Initialize using centroids found in pre-training.
        # Go through a single iteration of cluster assignment and centroid
        # recomputation.
        try:
            clusters, assignments = kmeans_cuda(X.astype(np.float32),
                                                init=km_model_pretrain.cluster_centers_[:, :-1],
                                                clusters=self.k,
                                                seed=self.random_state
                                                )
            km_model = KMeans(n_clusters=self.k)
            clusters[~np.isfinite(clusters)] = -1e9
            km_model.cluster_centers_ = clusters
        except:
            km_model = KMeans(n_clusters=self.k,
                              init=km_model_pretrain.cluster_centers_[:, :-1],
                              n_init=1,
                              max_iter=1,
                              n_jobs=self.n_jobs
                              )
            km_model.fit(X)

        self.km_model = km_model
        self.cluster_centers_ = km_model.cluster_centers_
        return self

    def transform(self, X, y=None):
        """Outputs the closest cluster id for each input data point.

        Parameters
        ----------
        X : array-like or sparse matrix, shape=(n_data_points, n_features)

        y : vector of length n_data_points, optional, default None
            Target vector is ignored even if provided.

        Returns
        -------
        cluster_ids : array, shape[n_data_points,1]
        """
        clusters = self.km_model.predict(X)
        return clusters.reshape(-1, 1)

    def fit_transform(self, X, y=None):
        """Runs fit followed by transform.
        """
        self.fit(X, y)
        return self.transform(X, y)