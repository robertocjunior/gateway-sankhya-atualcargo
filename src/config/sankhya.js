import dotenv from 'dotenv';
dotenv.config();

export const sankhyaConfig = {
  baseUrl: process.env.SANKHYA_URL,
  username: process.env.SANKHYA_USER,
  password: process.env.SANKHYA_PASSWORD,
};