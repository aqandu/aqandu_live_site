# stgp_v01_2
# spatial-temporal GP model
# v01: use SE kernel for space (no elevation) and SE kernel for time
#      stData must NOT contians NaN values.
# Tips: if there are, use interpolation in fill them if the the missing values are minor.
# v01_2: add elevation as model input.
#        lat & long share the same length sacle, elevation use its own length scale
#
# v02: modual version of v01_2
# %%
from torch import (
    einsum,
    LongTensor,
    index_select,
    cat,
    tensor,
    log,
    sum,
    exp,
    eye,
    symeig,
    no_grad,
    zeros,
)
from torch.optim import (
    LBFGS,
    Adam,
)
from torch.nn import (
    Module,
    Parameter,
)
from numpy import (
    concatenate,
    arange
)
from math import exp as math_exp

JITTER = 1e-1


def kronecker(A, B):
    AB = einsum("ab,cd->acbd", A, B)
    AB = AB.reshape(A.size(0) * B.size(0), A.size(1) * B.size(1))
    return AB


# torch repeat
def tile(a, dim, n_tile):
    init_dim = a.size(dim)
    repeat_idx = [1] * a.dim()
    repeat_idx[dim] = n_tile
    a = a.repeat(*(repeat_idx))
    order_index = LongTensor(concatenate([init_dim * arange(n_tile) + i for i in range(init_dim)]))
    return index_select(a, dim, order_index)


# combination using Kronecker product similar manner
def combinations(A, B):
    A1 = tile(A, 0, B.size(0))
    B1 = B.repeat(A.size(0), 1)
    return cat((A1, B1), dim=1)


class gaussian_model(Module):
    def __init__(self, space_coordinates, time_coordinates, stData,
                 latlong_length_scale=4300., elevation_length_scale=30., time_length_scale=0.25,
                 noise_variance=0.1, signal_variance=1.):
        # space_coordinates musth a matrix of [number of space_coordinates x (lat,long,elevation)]
        # in UTM or any meter coordinate.
        # time_coordinates musth a matrix of [number of time_coordinates x 1] in hour formate
        # stData musth be a matrix of [space_coordinates.size(0) x time_coordinates.size(0)]

        super(gaussian_model, self).__init__()
        self.space_coordinates = tensor(space_coordinates)
        self.time_coordinates = tensor(time_coordinates)
        self.stData = tensor(stData)
        self.log_latlong_length_scale = Parameter(log(tensor(latlong_length_scale)))
        self.log_elevation_length_scale = Parameter(log(tensor(elevation_length_scale)))
        self.log_time_length_scale = Parameter(log(tensor(time_length_scale)))
        self.log_noise_variance = Parameter(log(tensor(noise_variance)))
        self.log_signal_variance = Parameter(log(tensor(signal_variance)))

        self.update()

    def getLengthScales(self):
        return math_exp(self.log_latlong_length_scale), math_exp(self.log_elevation_length_scale), math_exp(self.log_time_length_scale)

    def SE_kernel(self, X, X2, length_scale):
        # length_scale MUST be positive
        X = X / length_scale.expand(X.size(0), X.size(1))
        X2 = X2 / length_scale.expand(X2.size(0), X2.size(1))

        X_norm2 = sum(X * X, dim=1).view(-1, 1)
        X2_norm2 = sum(X2 * X2, dim=1).view(-1, 1)

        # compute effective distance
        K = -2.0 * X @ X2.t() + X_norm2.expand(X.size(0), X2.size(0)) + X2_norm2.t().expand(X.size(0), X2.size(0))
        K = exp(-K) * 1.0
        return K

    def update(self):
        latlong_kernel = self.SE_kernel(self.space_coordinates[:, 0:2], self.space_coordinates[:, 0:2],
                                        exp(self.log_latlong_length_scale))
        elevation_kernel = self.SE_kernel(self.space_coordinates[:, 2:3], self.space_coordinates[:, 2:3],
                                          exp(self.log_elevation_length_scale))
        spatial_kernel = latlong_kernel * elevation_kernel + eye(latlong_kernel.size(0)) * JITTER

        temporal_kernel = self.SE_kernel(
            self.time_coordinates,
            self.time_coordinates,
            exp(self.log_time_length_scale)
        ) + eye(self.time_coordinates.size(0)) * JITTER

        eigen_value_s, eigen_vector_s = symeig(spatial_kernel, eigenvectors=True)
        eigen_value_t, eigen_vector_t = symeig(temporal_kernel, eigenvectors=True)

        eigen_vector_st = kronecker(eigen_vector_t, eigen_vector_s)
        eigen_value_st = kronecker(eigen_value_t.view(-1, 1), eigen_value_s.view(-1, 1)).view(-1)
        eigen_value_st_plus_noise_inverse = 1. / (eigen_value_st + exp(self.log_noise_variance))

        sigma_inverse = eigen_vector_st @ eigen_value_st_plus_noise_inverse.diag_embed() @ eigen_vector_st.transpose(-2,
                                                                                                                     -1)

        self.K = eigen_vector_st @ eigen_value_st.diag_embed() @ eigen_vector_st.transpose(-2, -1)
        self.sigma_inverse = sigma_inverse
        self.alpha = sigma_inverse @ self.stData.transpose(-2, -1).reshape(-1, 1)
        self.eigen_value_st = eigen_value_st

    def forward(self, test_space_coordinates, test_time_coordinates):
        with no_grad():
            test_latlong_kernel = self.SE_kernel(test_space_coordinates[:, 0:2], self.space_coordinates[:, 0:2],
                                                 exp(self.log_latlong_length_scale))
            test_elevation_kernel = self.SE_kernel(test_space_coordinates[:, 2:3], self.space_coordinates[:, 2:3],
                                                   exp(self.log_elevation_length_scale))
            test_spatial_kernel = test_latlong_kernel * test_elevation_kernel

            test_temporal_kernel = self.SE_kernel(test_time_coordinates, self.time_coordinates,
                                                  exp(self.log_time_length_scale))

            test_st_kernel = kronecker(test_temporal_kernel, test_spatial_kernel)
            yPred = test_st_kernel @ self.alpha

            yVar = zeros(test_st_kernel.size(0))
            for i in range(test_st_kernel.size(0)):
                yVar[i] = self.log_signal_variance.exp() - test_st_kernel[i:i + 1, :] @ self.sigma_inverse @ test_st_kernel[i:i + 1, :].t()

            yPred = yPred.view(test_time_coordinates.size(0), test_space_coordinates.size(0)).transpose(-2, -1)
            yVar = yVar.view(test_time_coordinates.size(0), test_space_coordinates.size(0)).transpose(-2, -1)
            return yPred, yVar

    def negative_log_likelihood(self):
        nll = 0
        nll += 0.5 * (self.eigen_value_st + exp(self.log_noise_variance)).log().sum()
        nll += 0.5 * (self.stData.transpose(-2, -1).reshape(1, -1) @ self.alpha).sum()
        return nll

    def train_bfgs(self, niteration, lr=0.001):
        # LBFGS optimizer
        optimizer = LBFGS(self.parameters(), lr=lr)  # lr is very important, lr>0.1 lead to failure

        # LBFGS
        def closure():
            optimizer.zero_grad()
            self.update()
            loss = self.negative_log_likelihood()
            loss.backward()
            print('nll:', loss.item())
            return loss
        for i in range(niteration):
            optimizer.step(closure)

    def train_adam(self, niteration, lr=0.001):
        # adam optimizer
        # uncommont the following to enable
        optimizer = Adam(self.parameters(), lr=lr)
        for i in range(niteration):
            optimizer.zero_grad()
            self.update()
            loss = self.negative_log_likelihood()
            loss.backward()
            optimizer.step()
            print('loss_nnl:', loss.item())
