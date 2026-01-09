import sys
import traceback

try:
    import pandas as pd
    import numpy as np
    from sklearn.model_selection import train_test_split, GridSearchCV
    from sklearn.linear_model import LinearRegression, Ridge, Lasso
    from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
    import statsmodels.api as sm
    from statsmodels.stats.stattools import durbin_watson
    from statsmodels.stats.diagnostic import het_breuschpagan
    from scipy import stats
    import joblib
except Exception as e:
    print("ImportError:", e)
    traceback.print_exc()
    sys.exit(1)

INPUT_CSV = '/workspaces/ARM_Gruppe-5/data/ARM_Master_Table_final.csv'
MODEL_PATH = '/workspaces/ARM_Gruppe-5/best_model_regression.joblib'
EXPORT_PATH = '/workspaces/ARM_Gruppe-5/exporte/predictions_regression.csv'

print('Lade Daten...')
df = pd.read_csv(INPUT_CSV)
cols = ['ds10200_quadratmeterpreis_chf', 'ds10680_endbestand']
for c in cols:
    if c not in df.columns:
        print(f"Spalte nicht gefunden: {c}")
        sys.exit(1)

df_model = df[cols].copy()
print('Shape (original):', df_model.shape)

# Konvertiere zu numerisch
for c in cols:
    df_model[c] = pd.to_numeric(df_model[c], errors='coerce')

print('Fehlende Werte vor dropna:\n', df_model.isnull().sum().to_dict())

df_model = df_model.dropna(subset=cols)
print('Shape (nach dropna):', df_model.shape)

# Entferne Null- oder Negativpreise
df_model = df_model[df_model['ds10200_quadratmeterpreis_chf'] > 0]
print('Shape (Preis > 0):', df_model.shape)

# Korrelation
corr = df_model['ds10200_quadratmeterpreis_chf'].corr(df_model['ds10680_endbestand'])
pearson_r, pval = stats.pearsonr(df_model['ds10200_quadratmeterpreis_chf'], df_model['ds10680_endbestand'])
print(f'Pearson r: {corr:.4f}, r(from scipy): {pearson_r:.4f}, p-value: {pval:.4e}')

# Features / Ziel
X = df_model[['ds10680_endbestand']].values
y = df_model['ds10200_quadratmeterpreis_chf'].values

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
print('Train size:', X_train.shape[0], 'Test size:', X_test.shape[0])

# Baseline Linear Regression
lr = LinearRegression()
lr.fit(X_train, y_train)
y_pred_lr = lr.predict(X_test)
print('\nLinear Regression coefficients:')
print('Intercept:', lr.intercept_)
print('Coef:', lr.coef_[0])

# Ridge & Lasso default
ridge = Ridge(alpha=1.0)
ridge.fit(X_train, y_train)
y_pred_ridge = ridge.predict(X_test)

lasso = Lasso(alpha=0.1, max_iter=10000)
lasso.fit(X_train, y_train)
y_pred_lasso = lasso.predict(X_test)

print('\nRidge coef:', ridge.coef_[0], 'Lasso coef:', lasso.coef_[0])

# GridSearchCV
alphas = [0.001, 0.01, 0.1, 1, 10, 100]
ridge_grid = GridSearchCV(Ridge(), param_grid={'alpha': alphas}, cv=5, scoring='r2')
ridge_grid.fit(X_train, y_train)
best_ridge = ridge_grid.best_estimator_

lasso_grid = GridSearchCV(Lasso(max_iter=20000), param_grid={'alpha': alphas}, cv=5, scoring='r2')
lasso_grid.fit(X_train, y_train)
best_lasso = lasso_grid.best_estimator_

print('\nGridSearch Ergebnisse:')
print('Ridge best alpha:', ridge_grid.best_params_, 'best CV R2:', ridge_grid.best_score_)
print('Lasso best alpha:', lasso_grid.best_params_, 'best CV R2:', lasso_grid.best_score_)

# Vorhersagen der besten Modelle
y_pred_ridge_best = best_ridge.predict(X_test)
y_pred_lasso_best = best_lasso.predict(X_test)

# Evaluation
def evaluate(y_true, y_pred):
    rmse = np.sqrt(mean_squared_error(y_true, y_pred))
    mae = mean_absolute_error(y_true, y_pred)
    r2 = r2_score(y_true, y_pred)
    return rmse, mae, r2

metrics = {}
metrics['Linear'] = evaluate(y_test, y_pred_lr)
metrics['Ridge'] = evaluate(y_test, y_pred_ridge)
metrics['Lasso'] = evaluate(y_test, y_pred_lasso)
metrics['Ridge_best'] = evaluate(y_test, y_pred_ridge_best)
metrics['Lasso_best'] = evaluate(y_test, y_pred_lasso_best)

print('\nEvaluationsmetriken (RMSE, MAE, R2):')
for k, v in metrics.items():
    print(f"{k}: RMSE={v[0]:.2f}, MAE={v[1]:.2f}, R2={v[2]:.4f}")

# Bestes Modell wählen nach R2
best_model_name = max(metrics.items(), key=lambda kv: kv[1][2])[0]
print('\nBestes Modell (nach R2):', best_model_name)

model_map = {
    'Linear': (lr, y_pred_lr),
    'Ridge': (ridge, y_pred_ridge),
    'Lasso': (lasso, y_pred_lasso),
    'Ridge_best': (best_ridge, y_pred_ridge_best),
    'Lasso_best': (best_lasso, y_pred_lasso_best)
}

best_model, best_pred = model_map[best_model_name]

# Residuenanalyse
residuals = y_test - best_pred
print('\nDurbin-Watson:', durbin_watson(residuals))
X_test_const = sm.add_constant(X_test)
lm_stat, lm_pvalue, f_stat, f_pvalue = het_breuschpagan(residuals, X_test_const)
print('Breusch-Pagan LM stat, p:', lm_stat, lm_pvalue)
print('Breusch-Pagan F stat, p:', f_stat, f_pvalue)

# Speichere Modell
joblib.dump(best_model, MODEL_PATH)
print('Gespeichertes Modell nach:', MODEL_PATH)

# Erstelle Predictions-DF
preds_df = pd.DataFrame({
    'ds10680_endbestand': X_test.flatten(),
    'y_true_ds10200_quadratmeterpreis_chf': y_test,
    'y_pred': best_pred,
    'residual': residuals
})

preds_df.to_csv(EXPORT_PATH, index=False)
print('Exportierte Vorhersagen nach:', EXPORT_PATH)

print('\nFertig.')
 
