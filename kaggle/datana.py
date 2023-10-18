import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.linear_model import LinearRegression, LogisticRegression
from sklearn.cluster import KMeans
from sklearn.model_selection import train_test_split
from sklearn.inspection import PartialDependenceDisplay

class Show:
    '''
    123
    '''
    model_dict = {
        'RFC': RandomForestClassifier,
        'RFR': RandomForestRegressor,
        'LR': LinearRegression,
        'LGR': LogisticRegression,
        'KMN': KMeans
        }

    def __init__(self, data: pd.DataFrame, model, target = None, model_para = None, random_state = 5):
        '''
        model: [
            'RFC' -> RandomForestClassifier,
            'RFR' -> RandomForestRegressor,
            'LR' -> LinearRegression,
            'LGR' -> LogisticRegression,
            'KMN' -> KMeans]
        '''
        self.data = data
        self.features = data.columns.drop(target) if target is not None else data.columns
        self.select_features = None
        self.target = target
        if model_para is not None:
            self.model = self.model_dict[model](random_state = random_state, **model_para)
        else:
            self.model = self.model_dict[model](random_state = random_state)
        self.train_x, self.cross_x, self.train_y, self.cross_y = \
            train_test_split(
            data.drop(target, axis = 1), 
            data[target], 
            test_size = 0.2, 
            random_state = random_state
            )
        self.model.fit(self.train_x, self.train_y)
        
    def importance(self, figsize = (10, 8), plt_para = None, n_selected_features = 'all', show = True) -> pd.Index:
        '''
        Show features importances barplot.

        `n_selected_features` = 'all' | int
            determine the number of the most reserved important features.
        `show`: bool
            whether to show barplot results or not.
        '''
        data = pd.DataFrame(
            {
            'feature': self.model.feature_names_in_,
            'importance': self.model.feature_importances_
            }
            ).sort_values(by = ['importance'])
        plt.figure(figsize = figsize, **plt_para) if plt_para is not None else plt.figure(figsize = figsize)
        if show:
            sns.barplot(data, y = 'feature', x = 'importance', orient = 'h')
        if n_selected_features == 'all':
            select_features = self.features
        elif isinstance(n_selected_features, int):
            select_features = data.feature[-n_selected_features:]
        else:
            raise TypeError("`n_selected_features` should be 'all' or numeral.")
        self.select_features = select_features
        return data

    def partial(self, figsize = (10, 8), n_features = 5):
        '''
        If show.n_selected_features is None, this function will call show.importance to get n_selected_features,
        the number is datermined by parameter `n_features`.

        This function finally uses self.select_features which is determined by self.importance function.

        But you can rewrite it by hand.

        but take care of that once the features amount to analysis is too large, sklearn.PartialDependenceDisplay runs slowly.
        '''
        plt.figure(figsize = figsize)
        if self.select_features is None:
            self.importance(n_selected_features = n_features)
        PartialDependenceDisplay.from_estimator(self.model, X = self.data.drop(['Class'], axis = 1), features = self.select_features)
        plt.subplots_adjust(wspace = 0.2, hspace = 0.8)
        plt.show()

    def corr(self, features = None, figsize = (10, 8)):
        '''
        Plot correlation heatmap between selected features.
        '''
        plt.figure(figsize = figsize)
        if features is None:
            sns.heatmap(self.data.corr(), annot = True)
        else:
            sns.heatmap(self.data[features].corr(), annot = True)

    def dist(self, features, test_data = None | pd.DataFrame, col = 4, bins = 50, wh = (0.3, 0.3), figsize = (10, 8)):
        '''
        Plot selected features' distribution histmap.
        Train data color -> Blue: #3386FF
        Test data color -> Yellow: #EFB000
        '''
        row = int(np.ceil(len(features) / col))
        fig, axs = plt.subplots(nrows = row, ncols = col, figsize = figsize)
        ws, hs = wh
        if test_data is not None:
            for i in features:
                if i not in test_data.columns:
                    raise Exception(f"feature {i} not in Test data, please check test data.")
        for feature, ax in zip(features, axs.flatten()):
            ax.set_title(f'{feature} dist')
            if test_data is None:
                sns.histplot(self.data[feature], ax = ax, kde = True, bins = bins, color = "#3386FF")
            else:
                sns.histplot(self.data[feature], ax = ax, label = 'Train', bins = bins, color = "#3386FF")
                sns.histplot(test_data[feature], ax = ax, label = 'Test', bins = bins, color = "#EFB000")
        plt.subplots_adjust(wspace = ws, hspace = hs)



        
'''
class Data:

    def __init__(self, data_path, )

'''