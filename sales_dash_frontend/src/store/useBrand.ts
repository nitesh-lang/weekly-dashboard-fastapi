import { create } from "zustand";
import { persist } from "zustand/middleware";

export interface BrandOption {
  key: string;
  label: string;
}

interface BrandState {
  brand: string;
  brands: BrandOption[];
  setBrand: (key: string) => void;
  setBrands: (opts: BrandOption[]) => void;
}

export const useBrand = create<BrandState>()(
  persist(
    (set) => ({
      brand: "nexlev",
      brands: [],
      setBrand: (key) => set({ brand: key }),
      setBrands: (opts) => set({ brands: opts }),
    }),
    { name: "sales-dashboard.brand" }
  )
);
