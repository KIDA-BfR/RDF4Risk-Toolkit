// Small, self-contained demo dataset for the "Load demo dataset" CTA. It travels through the
// real upload_file event/snapshot pipeline (not a mock), and is shaped to exercise the workflow:
// a comma-list column to split (Host range), a codes column to expand (Resistance), and numeric
// columns to omit (Concentration, Year).
export const DEMO_FILENAME = 'demo_sample_overview.csv';

export const DEMO_CSV = `Sample ID,Pathogen,Host,Resistance Codes,Sampling Country,Concentration (CFU/g),Year
S-001,Salmonella enterica,Gallus gallus,"CIP, TET",Germany,1200,2024
S-002,Campylobacter jejuni,Sus scrofa,"ERY",France,340,2024
S-003,Listeria monocytogenes,Bos taurus,"AMP, GEN",Germany,87,2023
S-004,Escherichia coli,Gallus gallus,"CIP, AMP, TET",Netherlands,2100,2024
S-005,Salmonella enterica,Sus scrofa,"TET",Spain,560,2023
S-006,Yersinia enterocolitica,Sus scrofa,"GEN",Germany,45,2024
S-007,Campylobacter coli,Gallus gallus,"ERY, CIP",Italy,780,2023
S-008,Listeria monocytogenes,Bos taurus,"AMP",France,130,2024
`;

export function demoCsvBase64(): string {
  // UTF-8 safe base64 of the demo CSV (ASCII content, so btoa is fine).
  return window.btoa(DEMO_CSV);
}
