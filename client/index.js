// Install axios if you haven't already:
// npm install axios

const axios = require('axios');


let config = {
  method: 'get',
  maxBodyLength: Infinity,
  url: 'http://localhost:3000/namespaces',
  headers: { 
    'Content-Type': 'application/json'
  }
};


// Function to make concurrent requests
async function sendConcurrentRequests() {
  const numRequests = 3;
  const requests = [];

  // Create an array of Axios promises
  for (let i = 0; i < numRequests; i++) {
    requests.push(axios.request(config)
    .then((response) => {
      console.log(JSON.stringify(response.data));
    })
    .catch((error) => {
      console.log(error);
    }));
  }

  try {
    // Execute all requests concurrently
    const responses = await Promise.all(requests);

    // Process the responses (e.g., log data, handle errors)
    responses.forEach((response, index) => {
      console.log(`Request ${index + 1} status: ${response.status}`);
      // Handle response data as needed
    });
  } catch (error) {
    console.error('Error making requests:', error.message);
  }
}

// Call the function to send concurrent requests
sendConcurrentRequests();
