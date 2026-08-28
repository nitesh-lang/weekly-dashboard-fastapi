export interface KpiBundle {
  monthly_target: number;
  target_till: number;
  actual: number;
  achievement: number;
  pace: number;
}

export interface DayWiseRow {
  date: string;
  net_sales: number;
  units: number;
  target: number;
  actual: number;
  achieved: number;
}

export interface MtdChart {
  labels: string[];
  actual: number[];
  target: number[];
}

export interface WeekWiseRow {
  week: string;
  net_sales: number;
}

export interface AsinRow {
  asin: string;
  model_no: string;
  category: string;
  product_name: string;
  target: number;
  actual: number;
  units_ordered: number;
  target_units: number;
  achievement: number;
  sparkline: string;
  out_of_plan: boolean;
}

export interface CategoryRow {
  category: string;
  target: number;
  actual: number;
  achievement: number;
  units_ordered?: number;
  [k: string]: unknown;
}

export interface DonutBundle {
  labels?: string[];
  values?: number[];
  data?: Array<{ label: string; value: number }>;
  [k: string]: unknown;
}

export interface MonthwiseChart {
  labels: string[];
  asins: string[];
  data: number[][];
}

export interface ModelTrendRow {
  model: string;
  category: string;
  s1: number;
  s2: number;
  s3: number;
  u1: number;
  u2: number;
  u3: number;
  sales_trend_pct: number;
  trend_class: string;
}

export interface ModelTrendBundle {
  months: string[];
  days?: number[];
  rows: ModelTrendRow[];
  sales_chart?: unknown;
  units_chart?: unknown;
}

export interface Validation {
  [k: string]: unknown;
}

export interface DashboardData extends KpiBundle {
  brand: { key: string; label: string };
  upload_enabled: boolean;
  sync_enabled: boolean;
  sp_api_enabled: boolean;
  plan_scope: boolean;
  plan_scope_active: boolean;
  plan_asin_count: number;
  months: string[];
  trend_months: string[];
  selected_month: string;
  from_date: string;
  to_date: string;
  trend_month: string;
  total_units_ordered: number;
  daywise: DayWiseRow[];
  chart: MtdChart;
  weekwise: WeekWiseRow[];
  asin_rows: AsinRow[];
  cat_rows: CategoryRow[];
  category_donut: DonutBundle | null;
  monthwise_chart: MonthwiseChart;
  model_trend: ModelTrendBundle | null;
  validation: Validation;
}
