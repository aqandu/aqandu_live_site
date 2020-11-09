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
import torch
import torch.nn as nn
import numpy as np
import math
import scipy


JITTER = 1e-6

# this does an eigen analysis of a symmetric circulant matrix using an FFT
#def symeigCirculant(data_first_row, eigenvectors=True):


#  This is a set of tools for supporting and inverting circulant matrices, as needed for efficient inversion of time series (sampled at regular intervals)

# efficient matrix multiply with diagonal matrix -- I cannot believe torch doesn't have this.
def diagMultTorchLeft(diag_vector, matrix):
    rows = diag_vector.shape[0]
    cols = matrix.shape[1]
#    print(diag_vector.shape)
#    print(matrix.shape)
    if (rows != matrix.shape[0]):
        print("RunTimeError: bad entries for diagonal matrix multiply")
        return torch.zeros([0])

    result = torch.zeros([rows,cols],dtype=torch.float64)
    for i in range(rows):
        result[i, :] = diag_vector[i]*matrix[i, :]
#    print(result.shape)
    return result

# used for plugging kernels into other operations associated with circulant kernel matrices
def gaussKernel(x):
    return(math.exp(-(x**2/2.0)))

# just fills up an array with kernel values, relative to the zero position and wrapping boundary conditions (circulant
def buildKernelArray(size, kernel, bandwidth=1.0):
    array = np.zeros(size)
    if (size % 2) != 0:
        for i in range((size-1)//2):
            array[i] = kernel(float(i)/bandwidth)
        for i in range((size+1)//2):
            array[size-(i+1)] = kernel(float((i+1))/bandwidth)
    else:
        for i in range((size)//2):
            array[i] = kernel(float(i)/bandwidth)
        for i in range((size)//2):
            array[size-(i+1)] = kernel(float((i+1))/bandwidth)
    return(array)


# convenience function for getting a circulant matrix
def buildKernelCirculantMatrix(size, kernel, bandwidth=1.0):
    return(scipy.linalg.circulant(buildKernelArray(size, kernel, bandwidth)))

#uses the fft to compute the inverse of a circulant matrix, specified by the first column as input
def circulantMatrixInverse(vector):
    v_fft = scipy.fft(vector)
    size = vector.shape[0]
    array = np.zeros([size, size], dtype=np.complex_)
    for i in range(size):
        array[:,i] = np.exp(-2j * np.pi * i * np.arange(size)/size)
    array *= 1./np.sqrt(size)
    return(np.matmul(np.matmul(array, np.diagflat(1.0/v_fft)), (array.conjugate()).transpose()))


#uses the fft to compute the complex eigen values/vectors of a circulant matrix, specified by the first column as input
def circulantMatrixEigen(vector):
    v_fft = scipy.fft.fft(vector)
    size = vector.shape[0]
    array = np.zeros([size, size], dtype=np.complex_)
    for i in range(size):
        array[:,i] = np.exp(-2j * np.pi * i * np.arange(size)/size)
    array *= 1./np.sqrt(size)
# return an complex valued eigen and vectors... 
    return(v_fft, array)

# #uses the fft to compute the complex eigen values/vectors of a circulant matrix, specified by the first column as input
# def circulantMatrixEigenTorch(vector):
#     v_fft = pytorch.fft(vector, 1, onesided=False)
#     size = vector.shape[0]
#     array = np.zeros([size, size], dtype=np.complex_)
#     for i in range(size):
#         array[:,i] = torch.exp(-2.j * np.pi * i * torch.arange(size)/size)
#     array *= 1./torch.sqrt(size)
# # return an complex valued eigen and vectors... 
#     return(v_fft, array)

# This works and has been tested
def symCirculantMatrixEigen(vector):
    v_fft = np.real(scipy.fft.fft(vector))
#    print("fft")
#    print(v_fft)
    size = vector.shape[0]
    array = np.zeros([size, size])
    for i in range(size):
        this_vector = np.exp(-2j * np.pi * i * np.arange(size)/(size))
        if i <= size//2:
            array[:,i] = np.real(this_vector)
        else:
            array[:,i] = np.imag(this_vector)
#crazy normalization of the high-freq vector for special, even case
    if (size % 2) == 0:
        array[:,size//2] *= np.sqrt(0.5)
##  This is an adjustment , like with the DCT, for only taking real values
##     array *= math.sqrt(2.)/np.sqrt(size)
    array *= np.sqrt(2.)/np.sqrt(size)
    array[:, 0] *= 1.0/np.sqrt(2.)
# return an complex valued eigen and vectors... 
    return(v_fft, array)


####  end of code to support circulant matrices

def kronecker(A, B):
    AB = torch.einsum("ab,cd->acbd", A, B)
    AB = AB.reshape(A.size(0) * B.size(0), A.size(1) * B.size(1))
    return AB


# torch repeat
def tile(a, dim, n_tile):
    init_dim = a.size(dim)
    repeat_idx = [1] * a.dim()
    repeat_idx[dim] = n_tile
    a = a.repeat(*(repeat_idx))
    order_index = torch.LongTensor(np.concatenate([init_dim * np.arange(n_tile) + i for i in range(init_dim)]))
    return torch.index_select(a, dim, order_index)


# combination using Kronecker product similar manner
def combinations(A, B):
    A1 = tile(A, 0, B.size(0))
    B1 = B.repeat(A.size(0), 1)
    return torch.cat((A1, B1), dim=1)


# USAGE sequence for this class
# 1) constructor (with sensor data)
# 2) forward

# Ross - Sept 2020
# 
# This model now has a "time_structured" flag.  By default (true), it assumes that the time part of the process is sampled on a regular 1D grid, and that it is padded well
# enough to allow circular/cyclic boundary conditions.  In that case, the fft is used to decompose the time part of the matrix.  If structured=False, then it does not assume this
# and needs to do a full SVD on that big matrix.   The structured case should be much faster for longer time intervals.
#
# Also fixed problems in the kernel calculation (missing factor of 2)
# Put in lots of print statements that are commented out, that were used for debugging.
# Tested/debugged the case where query is multiple spatial locations -- this will be used in getEstimateForLocations and getEstimateMap in the API code
#

class gaussian_model(nn.Module):
    def __init__(self, space_coordinates, time_coordinates, stData,
                 latlon_length_scale=4300., elevation_length_scale=30., time_length_scale=0.25,
                 noise_variance=0.1, signal_variance=1., time_structured=True):
        # space_coordinates musth a matrix of [number of space_coordinates x (lat,long,elevation)]
        # in UTM or any meter coordinate.
        # time_coordinates musth a matrix of [number of time_coordinates x 1] in hour formate
        # stData musth be a matrix of [space_coordinates.size(0) x time_coordinates.size(0)]

        super(gaussian_model, self).__init__()
        self.space_coordinates = torch.tensor(space_coordinates)
        self.time_coordinates = torch.tensor(time_coordinates)
        self.stData = torch.tensor(stData)
        self.log_latlon_length_scale = nn.Parameter(torch.log(torch.tensor(latlon_length_scale)))
        self.log_elevation_length_scale = nn.Parameter(torch.log(torch.tensor(elevation_length_scale)))
        self.log_time_length_scale = nn.Parameter(torch.log(torch.tensor(time_length_scale)))
        self.log_noise_variance = nn.Parameter(torch.log(torch.tensor(noise_variance)))
        self.log_signal_variance = nn.Parameter(torch.log(torch.tensor(signal_variance)))
        # this says whether or not you can use the FFT for time
        self.time_structured = time_structured

# build and invert kernel matrix        
        self.update()

    def getLengthScales(self):
        return math.exp(self.log_latlon_length_scale), math.exp(self.log_elevation_length_scale), math.exp(self.log_time_length_scale)

    def SE_kernel(self, X, X2, length_scale):
        # length_scale MUST be positive
        X = X / length_scale.expand(X.size(0), X.size(1))
        X2 = X2 / length_scale.expand(X2.size(0), X2.size(1))

        X_norm2 = torch.sum(X * X, dim=1).view(-1, 1)
        X2_norm2 = torch.sum(X2 * X2, dim=1).view(-1, 1)

        # compute effective distance
        K = -2.0 * X @ X2.t() + X_norm2.expand(X.size(0), X2.size(0)) + X2_norm2.t().expand(X.size(0), X2.size(0))
        K = torch.exp(K/(-2.)) * 1.0
        return K

    def update(self):
        latlon_kernel = self.SE_kernel(self.space_coordinates[:, 0:2], self.space_coordinates[:, 0:2],
                                        torch.exp(self.log_latlon_length_scale))
        elevation_kernel = self.SE_kernel(self.space_coordinates[:, 2:3], self.space_coordinates[:, 2:3],
                                          torch.exp(self.log_elevation_length_scale))
        spatial_kernel = latlon_kernel * elevation_kernel + torch.eye(latlon_kernel.size(0)) * JITTER

        eigen_value_s, eigen_vector_s = torch.symeig(spatial_kernel, eigenvectors=True)
        
        if not self.time_structured:
            temporal_kernel = self.SE_kernel(
                self.time_coordinates,
                self.time_coordinates,
                torch.exp(self.log_time_length_scale)
                ) + torch.eye(self.time_coordinates.size(0)) * JITTER
            np.savetxt('temp_kernel_unstructured.csv', (temporal_kernel).detach().numpy(), delimiter = ';')
            eigen_value_t, eigen_vector_t = torch.symeig(temporal_kernel, eigenvectors=True)
            eigen_vector_st = kronecker(eigen_vector_t, eigen_vector_s)
            eigen_value_st = kronecker(eigen_value_t.view(-1, 1), eigen_value_s.view(-1, 1)).view(-1)
            eigen_value_st_plus_noise_inverse = 1. / (self.log_signal_variance.exp()*eigen_value_st + torch.exp(self.log_noise_variance))
            sigma_inverse = eigen_vector_st @ eigen_value_st_plus_noise_inverse.diag_embed() @ (eigen_vector_st.transpose(-2, -1))
#            self.K = eigen_vector_st @ eigen_value_st.diag_embed() @ eigen_vector_st.transpose(-2, -1)
 #           np.savetxt('kernel_unstructured.csv', (self.K).detach().numpy(), delimiter = ';')
            self.alpha = sigma_inverse @ self.stData.transpose(-2, -1).reshape(-1, 1)
            self.sigma_inverse = sigma_inverse
        else:
            # in this case we assume that time has a constant interval between successive samples
            # we might not need to build the matrix
            # right now do everything in numpy, until we have proper support in pytorch
            delta_time = self.time_coordinates[1] - self.time_coordinates[0]
# Probably don't need this...
            # temporal_kernel = buildKernelCirculantMatrix(self.time_coordinates.shape[0], gaussKernel,
            # #                                                  # express the length in terms of bins
            #                                                  torch.exp(self.log_time_length_scale)/delta_time)
            # this is the first column of the circulant time-kernel matrix
            temporal_kernel_vector = buildKernelArray(self.time_coordinates.shape[0], gaussKernel,
                                                             # express the length in terms of bins
                                                             torch.exp(self.log_time_length_scale)/delta_time)
#            print("about to do circ eigen")
            eigen_value_t_np, eigen_vector_t_np = symCirculantMatrixEigen(temporal_kernel_vector)
#            print("done circ eigen")

            eigen_value_t = torch.from_numpy(eigen_value_t_np)
            eigen_vector_t = torch.from_numpy(eigen_vector_t_np)
#            print("temp inversion")
# this is a test to make sure they are eigen vectors            
#            print(eigen_vector_t_np.transpose()@temporal_kernel@eigen_vector_t_np)                


            self.eigen_vector_st = kronecker(eigen_vector_t, eigen_vector_s)
#            eigen_value_st = kronecker(eigen_value_t.view(-1, 1), eigen_value_s.view(-1, 1)).view(-1)
            self.eigen_value_st = kronecker(eigen_value_t.view(-1, 1), eigen_value_s.view(-1, 1)).view(-1)
#            print("done kronecker products")
            self.eigen_value_st_plus_noise_inverse = 1. / (self.log_signal_variance.exp()*self.eigen_value_st + torch.exp(self.log_noise_variance))
#            sigma_inverse = eigen_vector_st @ eigen_value_st_plus_noise_inverse.diag_embed() @ (eigen_vector_st.transpose(-2, -1))
#            self.K = eigen_vector_st @ eigen_value_st.diag_embed() @ eigen_vector_st.transpose(-2, -1)
#            print("done computing vectors")

#            self.K = torch.from_numpy(K)
#            
#            eigen_value_st = torch.from_numpy(np.real(eigen_value_st_np))
#            print("done conversion to torch")
            

#        self.sigma_inverse = sigma_inverse
#        self.alpha = sigma_inverse @ self.stData.transpose(-2, -1).reshape(-1, 1)
#        self.eigen_value_st = eigen_value_st

    def forward(self, test_space_coordinates, test_time_coordinates):
        with torch.no_grad():
            test_latlon_kernel = self.SE_kernel(test_space_coordinates[:, 0:2], self.space_coordinates[:, 0:2],
                                                 torch.exp(self.log_latlon_length_scale))
            test_elevation_kernel = self.SE_kernel(test_space_coordinates[:, 2:3], self.space_coordinates[:, 2:3],
                                                   torch.exp(self.log_elevation_length_scale))
            test_spatial_kernel = test_latlon_kernel * test_elevation_kernel

            test_temporal_kernel = self.SE_kernel(test_time_coordinates, self.time_coordinates,
                                                  torch.exp(self.log_time_length_scale))

            test_st_kernel = self.log_signal_variance.exp()*kronecker(test_temporal_kernel, test_spatial_kernel)
            # alpha is the kernel inverse times the measurements that were taken already
            #        self.alpha = sigma_inverse @ self.stData.transpose(-2, -1).reshape(-1, 1)
            if self.time_structured==True:
                sigma_diag = diagMultTorchLeft(self.eigen_value_st_plus_noise_inverse, ((self.eigen_vector_st).transpose(-2, -1)@self.stData.transpose(-2, -1).reshape(-1, 1)))
#                print("done with sigma_diag")
                yPred = (test_st_kernel@self.eigen_vector_st)@sigma_diag
#                print("done with yPred")
                yVar = torch.zeros(test_st_kernel.size(0))
                test_times_eigen = test_st_kernel@ self.eigen_vector_st
                # for i in range(test_st_kernel.size(0)):
                #     yVar[i] = self.log_signal_variance.exp() - test_times_eigen[i:i+1, :] @diagMultTorchLeft(self.eigen_value_st_plus_noise_inverse, test_times_eigen[i:i+1, :] .t())
#                yVar = torch.diagonal(self.log_signal_variance.exp()*torch.eye(test_st_kernel.size(0)) - test_times_eigen@diagMultTorchLeft(self.eigen_value_st_plus_noise_inverse, test_times_eigen.t()))
                yVar = self.log_signal_variance.exp()*torch.ones(test_st_kernel.size(0)) - torch.einsum("ij,ji->i", test_times_eigen, diagMultTorchLeft(self.eigen_value_st_plus_noise_inverse, test_times_eigen.t()))
#                print(test_times_eigen.shape)
#                print(diagMultTorchLeft(self.eigen_value_st_plus_noise_inverse, test_times_eigen.t()))

#                print("done with yVar")

                yPred = yPred.view(test_time_coordinates.size(0), test_space_coordinates.size(0)).transpose(-2, -1)
                yVar = yVar.view(test_time_coordinates.size(0), test_space_coordinates.size(0)).transpose(-2, -1)

            else:
                yPred = test_st_kernel @ self.alpha

                yVar = torch.zeros(test_st_kernel.size(0))
                for i in range(test_st_kernel.size(0)):
                    yVar[i] = self.log_signal_variance.exp() - test_st_kernel[i:i + 1, :] @ self.sigma_inverse @ test_st_kernel[i:i + 1, :].t()

                yPred = yPred.view(test_time_coordinates.size(0), test_space_coordinates.size(0)).transpose(-2, -1)
                yVar = yVar.view(test_time_coordinates.size(0), test_space_coordinates.size(0)).transpose(-2, -1)

            return yPred, yVar
            
    def negative_log_likelihood(self):
        nll = 0
        nll += 0.5 * (self.eigen_value_st + torch.exp(self.log_noise_variance)).log().sum()
        nll += 0.5 * (self.stData.transpose(-2, -1).reshape(1, -1) @ self.alpha).sum()
        return nll

    def train_bfgs(self, niteration, lr=0.001):
        # LBFGS optimizer
        optimizer = torch.optim.LBFGS(self.parameters(), lr=lr)  # lr is very important, lr>0.1 lead to failure

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
        optimizer = torch.optim.Adam(self.parameters(), lr=lr)
        for i in range(niteration):
            optimizer.zero_grad()
            self.update()
            loss = self.negative_log_likelihood()
            loss.backward()
            optimizer.step()
            print('loss_nnl:', loss.item())
