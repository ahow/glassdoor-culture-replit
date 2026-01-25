"""
Performance Analysis Module
Correlates culture metrics with business performance data
"""

import os
import pandas as pd
import numpy as np
from scipy import stats
from typing import Dict, List, Optional, Tuple
import logging

logger = logging.getLogger(__name__)

EXCEL_PATH = 'attached_assets/asset_manager_comprehensive_database_1769351810411.xlsx'

COMPANY_NAME_MAPPING = {
    'State Street (Corp)': 'State Street',
    'Goldman Sachs Group': 'Goldman Sachs Group',
    'Morgan Stanley Inv. Mgmt.': 'Morgan Stanley',
    'Legal & General Group': 'Legal & General',
    'J.P. Morgan Chase': 'J.P. Morgan Chase',
    'Fidelity Investments': 'Fidelity Investments',
}

BUSINESS_MODEL_CATEGORIES = {
    'Traditional': 'Traditional/diversified asset manager',
    'Alternative': 'Alt mgr: Higher fees, lower fee-earning %',
    'Insurance/Wealth': 'Includes wealth mgmt/insurance revenue',
}

class PerformanceAnalyzer:
    def __init__(self, excel_path: str = EXCEL_PATH):
        self.excel_path = excel_path
        self.aum_data = None
        self.financials_data = None
        self.business_perf_data = None
        self.shareholder_data = None
        self.loaded = False
        
    def load_data(self) -> bool:
        try:
            if not os.path.exists(self.excel_path):
                logger.error(f"Performance data file not found: {self.excel_path}")
                return False
                
            xl = pd.ExcelFile(self.excel_path)
            
            self.aum_data = pd.read_excel(xl, sheet_name='AUM Data')
            self.financials_data = pd.read_excel(xl, sheet_name='Financials & Profitability')
            self.business_perf_data = pd.read_excel(xl, sheet_name='Business Performance')
            self.shareholder_data = pd.read_excel(xl, sheet_name='Shareholder Returns')
            
            self._clean_data()
            self.loaded = True
            logger.info(f"Loaded performance data: {len(self.business_perf_data)} companies")
            return True
        except Exception as e:
            logger.error(f"Error loading performance data: {e}")
            return False
    
    def _clean_data(self):
        if self.business_perf_data is not None:
            self.business_perf_data = self.business_perf_data[
                self.business_perf_data['Company'].notna() & 
                ~self.business_perf_data['Company'].astype(str).str.contains(
                    'EXPLAINED|METRICS|ROE|Revenue Yield|Fee-Earning|Operating Margin|Net Margin', 
                    na=False, regex=True
                )
            ].copy()
            
        if self.financials_data is not None:
            self.financials_data = self.financials_data[
                self.financials_data['Company'].notna()
            ].copy()
            
        if self.shareholder_data is not None:
            self.shareholder_data = self.shareholder_data[
                self.shareholder_data['Company'].notna()
            ].copy()
            
        if self.aum_data is not None:
            self.aum_data = self.aum_data[
                self.aum_data['Company'].notna()
            ].copy()
    
    def normalize_company_name(self, name: str) -> str:
        if name in COMPANY_NAME_MAPPING:
            return COMPANY_NAME_MAPPING[name]
        return name
    
    def get_business_model(self, company: str) -> str:
        if self.business_perf_data is None:
            return 'Unknown'
        row = self.business_perf_data[self.business_perf_data['Company'] == company]
        if row.empty:
            return 'Unknown'
        notes = row['Notes'].values[0]
        if pd.isna(notes):
            return 'Unknown'
        if 'Alt mgr' in str(notes):
            return 'Alternative'
        elif 'insurance' in str(notes).lower() or 'wealth' in str(notes).lower():
            return 'Insurance/Wealth'
        else:
            return 'Traditional'
    
    def get_performance_metrics(self, company: str) -> Optional[Dict]:
        if not self.loaded:
            self.load_data()
        
        normalized = self.normalize_company_name(company)
        metrics = {'company': company, 'matched_name': normalized}
        
        if self.business_perf_data is not None:
            row = self.business_perf_data[self.business_perf_data['Company'] == normalized]
            if not row.empty:
                metrics['roe_2024'] = row['2024 ROE (%)'].values[0] if not pd.isna(row['2024 ROE (%)'].values[0]) else None
                metrics['roe_5y_avg'] = row['5Y Avg ROE (%)'].values[0] if not pd.isna(row['5Y Avg ROE (%)'].values[0]) else None
                metrics['aum_2024'] = row['2024 AUM ($bn)'].values[0] if not pd.isna(row['2024 AUM ($bn)'].values[0]) else None
                metrics['rev_yield_bps'] = row['Rev Yield (bps)'].values[0] if not pd.isna(row['Rev Yield (bps)'].values[0]) else None
                metrics['business_model'] = self.get_business_model(normalized)
        
        if self.financials_data is not None:
            row = self.financials_data[self.financials_data['Company'] == normalized]
            if not row.empty:
                metrics['rev_cagr_5y'] = row['5Y Rev CAGR'].values[0] if '5Y Rev CAGR' in row.columns and not pd.isna(row['5Y Rev CAGR'].values[0]) else None
                metrics['op_margin_2024'] = row['2024 Op Margin'].values[0] if '2024 Op Margin' in row.columns and not pd.isna(row['2024 Op Margin'].values[0]) else None
                metrics['op_margin_5y_avg'] = row['5Y Avg Op Margin'].values[0] if '5Y Avg Op Margin' in row.columns and not pd.isna(row['5Y Avg Op Margin'].values[0]) else None
                metrics['net_margin_2024'] = row['2024 Net Margin'].values[0] if '2024 Net Margin' in row.columns and not pd.isna(row['2024 Net Margin'].values[0]) else None
        
        if self.shareholder_data is not None:
            row = self.shareholder_data[self.shareholder_data['Company'] == normalized]
            if not row.empty:
                metrics['tsr_cagr_5y'] = row['5Y TSR CAGR (%)'].values[0] if not pd.isna(row['5Y TSR CAGR (%)'].values[0]) else None
                metrics['market_cap_2024'] = row['2024 Market Cap ($bn)'].values[0] if not pd.isna(row['2024 Market Cap ($bn)'].values[0]) else None
                metrics['dividend_yield'] = row['2024 Dividend Yield (%)'].values[0] if not pd.isna(row['2024 Dividend Yield (%)'].values[0]) else None
        
        if self.aum_data is not None:
            row = self.aum_data[self.aum_data['Company'] == normalized]
            if not row.empty:
                metrics['aum_cagr_5y'] = row['5Y CAGR'].values[0] if not pd.isna(row['5Y CAGR'].values[0]) else None
        
        return metrics if len(metrics) > 2 else None
    
    def calculate_composite_score(self, metrics: Dict, peer_stats: Dict) -> Optional[float]:
        if not metrics:
            return None
        
        score_components = []
        weights = []
        
        if metrics.get('roe_5y_avg') is not None:
            peer_mean = peer_stats.get('roe_mean', 15)
            peer_std = peer_stats.get('roe_std', 5)
            if peer_std > 0:
                z_score = (metrics['roe_5y_avg'] - peer_mean) / peer_std
                score_components.append(max(-2, min(2, z_score)))
                weights.append(0.30)
        
        if metrics.get('aum_cagr_5y') is not None:
            peer_mean = peer_stats.get('aum_cagr_mean', 0.08)
            peer_std = peer_stats.get('aum_cagr_std', 0.05)
            if peer_std > 0:
                z_score = (metrics['aum_cagr_5y'] - peer_mean) / peer_std
                score_components.append(max(-2, min(2, z_score)))
                weights.append(0.25)
        
        if metrics.get('tsr_cagr_5y') is not None:
            peer_mean = peer_stats.get('tsr_mean', 10)
            peer_std = peer_stats.get('tsr_std', 15)
            if peer_std > 0:
                z_score = (metrics['tsr_cagr_5y'] - peer_mean) / peer_std
                score_components.append(max(-2, min(2, z_score)))
                weights.append(0.25)
        
        if metrics.get('op_margin_5y_avg') is not None:
            peer_mean = peer_stats.get('margin_mean', 0.30)
            peer_std = peer_stats.get('margin_std', 0.10)
            if peer_std > 0:
                z_score = (metrics['op_margin_5y_avg'] - peer_mean) / peer_std
                score_components.append(max(-2, min(2, z_score)))
                weights.append(0.20)
        
        if not score_components:
            return None
        
        total_weight = sum(weights)
        if total_weight == 0:
            return None
        
        weighted_score = sum(s * w for s, w in zip(score_components, weights)) / total_weight
        normalized_score = 50 + (weighted_score * 25)
        return max(0, min(100, normalized_score))
    
    def _is_numeric(self, val) -> bool:
        if pd.isna(val):
            return False
        if isinstance(val, (int, float, np.floating, np.integer)):
            return True
        return False
    
    def get_peer_statistics(self, business_model: str = None) -> Dict:
        if not self.loaded:
            self.load_data()
        
        roe_values = []
        aum_cagr_values = []
        tsr_values = []
        margin_values = []
        
        if self.business_perf_data is not None:
            for _, row in self.business_perf_data.iterrows():
                if business_model and self.get_business_model(row['Company']) != business_model:
                    continue
                val = row.get('5Y Avg ROE (%)')
                if self._is_numeric(val):
                    roe_values.append(float(val))
        
        if self.aum_data is not None:
            for _, row in self.aum_data.iterrows():
                val = row.get('5Y CAGR')
                if self._is_numeric(val):
                    aum_cagr_values.append(float(val))
        
        if self.shareholder_data is not None:
            for _, row in self.shareholder_data.iterrows():
                val = row.get('5Y TSR CAGR (%)')
                if self._is_numeric(val):
                    tsr_values.append(float(val))
        
        if self.financials_data is not None:
            for _, row in self.financials_data.iterrows():
                val = row.get('5Y Avg Op Margin')
                if self._is_numeric(val):
                    margin_values.append(float(val))
        
        return {
            'roe_mean': np.mean(roe_values) if roe_values else 15,
            'roe_std': np.std(roe_values) if len(roe_values) > 1 else 5,
            'aum_cagr_mean': np.mean(aum_cagr_values) if aum_cagr_values else 0.08,
            'aum_cagr_std': np.std(aum_cagr_values) if len(aum_cagr_values) > 1 else 0.05,
            'tsr_mean': np.mean(tsr_values) if tsr_values else 10,
            'tsr_std': np.std(tsr_values) if len(tsr_values) > 1 else 15,
            'margin_mean': np.mean(margin_values) if margin_values else 0.30,
            'margin_std': np.std(margin_values) if len(margin_values) > 1 else 0.10,
        }
    
    def calculate_correlation(self, culture_data: List[Dict], performance_data: List[Dict]) -> Dict:
        culture_dimensions = [
            'process_results', 'job_employee', 'professional_parochial',
            'open_closed', 'tight_loose', 'pragmatic_normative'
        ]
        mit_dimensions = [
            'agility', 'collaboration', 'customer_orientation', 'diversity',
            'execution', 'innovation', 'integrity', 'performance', 'respect'
        ]
        performance_metrics = [
            'roe_5y_avg', 'aum_cagr_5y', 'tsr_cagr_5y', 'op_margin_5y_avg', 'composite_score'
        ]
        
        results = {
            'hofstede': {},
            'mit': {},
            'summary': {
                'strongest_positive': [],
                'strongest_negative': [],
                'sample_size': 0
            }
        }
        
        company_data = {}
        for cd in culture_data:
            company_data[cd['company']] = {'culture': cd}
        for pd_item in performance_data:
            if pd_item['company'] in company_data:
                company_data[pd_item['company']]['performance'] = pd_item
        
        valid_companies = [c for c, d in company_data.items() if 'culture' in d and 'performance' in d]
        results['summary']['sample_size'] = len(valid_companies)
        
        if len(valid_companies) < 5:
            logger.warning(f"Insufficient data for correlation: {len(valid_companies)} companies")
            return results
        
        all_correlations = []
        
        for dim in culture_dimensions:
            results['hofstede'][dim] = {}
            for metric in performance_metrics:
                x_vals = []
                y_vals = []
                for company in valid_companies:
                    culture_val = company_data[company]['culture'].get('hofstede', {}).get(dim, {}).get('value')
                    perf_val = company_data[company]['performance'].get(metric)
                    if culture_val is not None and perf_val is not None:
                        x_vals.append(culture_val)
                        y_vals.append(perf_val)
                
                if len(x_vals) >= 5:
                    try:
                        corr, p_value = stats.pearsonr(x_vals, y_vals)
                        corr_val = float(corr)
                        p_val = float(p_value)
                        results['hofstede'][dim][metric] = {
                            'correlation': round(corr_val, 3),
                            'p_value': round(p_val, 4),
                            'significant': bool(p_val < 0.05),
                            'sample_size': len(x_vals)
                        }
                        all_correlations.append({
                            'framework': 'Hofstede',
                            'dimension': dim,
                            'metric': metric,
                            'correlation': corr_val,
                            'p_value': p_val
                        })
                    except Exception as e:
                        logger.error(f"Correlation error for {dim}/{metric}: {e}")
        
        for dim in mit_dimensions:
            results['mit'][dim] = {}
            for metric in performance_metrics:
                x_vals = []
                y_vals = []
                for company in valid_companies:
                    culture_val = company_data[company]['culture'].get('mit', {}).get(dim, {}).get('value')
                    perf_val = company_data[company]['performance'].get(metric)
                    if culture_val is not None and perf_val is not None:
                        x_vals.append(culture_val)
                        y_vals.append(perf_val)
                
                if len(x_vals) >= 5:
                    try:
                        corr, p_value = stats.pearsonr(x_vals, y_vals)
                        corr_val = float(corr)
                        p_val = float(p_value)
                        results['mit'][dim][metric] = {
                            'correlation': round(corr_val, 3),
                            'p_value': round(p_val, 4),
                            'significant': bool(p_val < 0.05),
                            'sample_size': len(x_vals)
                        }
                        all_correlations.append({
                            'framework': 'MIT',
                            'dimension': dim,
                            'metric': metric,
                            'correlation': corr_val,
                            'p_value': p_val
                        })
                    except Exception as e:
                        logger.error(f"Correlation error for {dim}/{metric}: {e}")
        
        if all_correlations:
            sorted_corrs = sorted(all_correlations, key=lambda x: x['correlation'], reverse=True)
            results['summary']['strongest_positive'] = sorted_corrs[:5]
            results['summary']['strongest_negative'] = sorted_corrs[-5:][::-1]
        
        return results


performance_analyzer = PerformanceAnalyzer()
