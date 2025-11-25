FROM node:18-alpine

# Set working folder inside the container
WORKDIR /app

# Copy package.json (and package-lock.json if present)
COPY package*.json ./

# Install only production deps
RUN npm install --production

# Copy the rest of your code (index.js, etc.)
COPY . .

# Fly will pass PORT, so listen on it
ENV PORT=3000
EXPOSE 3000

# Start the app
CMD ["node", "index.js"]
