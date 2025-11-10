import dotenv from 'dotenv';
dotenv.config();

export const sankhyaConfig = {
  url: process.env.SANKHYA_URL,
  contingencyUrl: process.env.SANKHYA_CONTINGENCY_URL || null,
  username: process.env.SANKHYA_USER,
  password: process.env.SANKHYA_PASSWORD,
  iscaDatasetId: process.env.SANKHYA_ISCA_DATASET_ID || '02S',
};