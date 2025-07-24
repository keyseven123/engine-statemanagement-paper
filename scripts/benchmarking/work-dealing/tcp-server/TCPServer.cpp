#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <iostream>
#include <random>
#include <string>
#include <thread>
#include <vector>
#include <unistd.h>
#include <arpa/inet.h>

class ClientHandler
{
public:
    ClientHandler(const int clientSocket, const sockaddr_in address, const double emitRate)
        : clientSocket(clientSocket), address(address), emitRate(emitRate)
    {
    }

    void handle()
    {
        std::cout << "New connection from " << inet_ntoa(address.sin_addr) << std::endl;
        try
        {
            std::random_device rd;
            std::mt19937_64 gen(rd());
            std::uniform_int_distribution<uint64_t> valueDistrib(0, UINT64_MAX);
            std::uniform_int_distribution<> counterDistrib(0, 9999);
            int tupleCounter = 0;
            constexpr auto checkTimeEvery = 1000L;
            std::stringstream messageStream;

            auto startTime = std::chrono::steady_clock::now();
            while (running)
            {
                counter = counterDistrib(gen);
                value = valueDistrib(gen);
                messageStream << std::to_string(counter) << "," << std::to_string(value) << "," << std::to_string(timestamp) << "\n";

                // Increment the tuple counter
                tupleCounter++;

                // Check if 1000 tuples have been sent
                if (tupleCounter >= checkTimeEvery)
                {
                    std::string message = messageStream.str();
                    send(clientSocket, message.c_str(), message.size(), 0);
                    messageStream.str("");
                    messageStream.clear();

                    auto endTime = std::chrono::steady_clock::now();
                    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime).count();
                    const auto delay = static_cast<long>(checkTimeEvery / emitRate);

                    if (elapsed < delay)
                    {
                        std::this_thread::sleep_for(std::chrono::milliseconds(delay - elapsed));
                    }

                    // Reset the tuple counter and update startTime
                    tupleCounter = 0;
                    startTime = std::chrono::steady_clock::now();
                }

                timestamp += 1;
            }
        }
        catch (const std::exception& e)
        {
            std::cerr << "Exception in client: " << e.what() << std::endl;
        }
        catch (...)
        {
            std::cout << "Client " << inet_ntoa(address.sin_addr) << " disconnected" << std::endl;
        }
        cleanup();
    }

    void cleanup()
    {
        running = false;
        close(clientSocket);
        std::cout << "Cleaned up connection from " << inet_ntoa(address.sin_addr) << std::endl;
    }

    [[nodiscard]] bool isRunning() const { return running; }

private:
    int clientSocket;
    sockaddr_in address;
    double emitRate; // tuples per second
    uint64_t counter;
    uint64_t value;
    int timestamp = 0;
    bool running = true;
};

class CounterServer
{
public:
    CounterServer(const std::string& host, const int port, const double emitRate) : host(host), port(port), emitRate(emitRate) { }

    void start()
    {
        if ((serverSocket = socket(AF_INET, SOCK_STREAM, 0)) == -1)
        {
            std::cerr << "Failed to create server socket" << std::endl;
            cleanup(-1);
        }

        const int opt = 1;
        setsockopt(serverSocket, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

        serverAddress.sin_family = AF_INET;
        serverAddress.sin_addr.s_addr = inet_addr(host.c_str());
        serverAddress.sin_port = htons(port);

        const int success = bind(serverSocket, (struct sockaddr*)&serverAddress, sizeof(serverAddress));
        if (success != 0)
        {
            std::cout << "\nShutting down server because it can not bind to server socket " << serverSocket << "..." << std::endl;
            cleanup(-1);
        }

        listen(serverSocket, 5);
        std::cout << "Server listening on " << host << ":" << port << std::endl;
        std::cout << "Emit rate: " << emitRate << " tuples/second" << std::endl;

        try
        {
            while (running)
            {
                sockaddr_in clientAddress;
                socklen_t clientLen = sizeof(clientAddress);
                const int clientSocket = accept(serverSocket, (struct sockaddr*)&clientAddress, &clientLen);
                ClientHandler client(clientSocket, clientAddress, emitRate);
                std::thread clientThread(&ClientHandler::handle, &client);
                clientThread.detach();
                clients.push_back(std::move(client));

                // Clean up disconnected clients
                auto it = clients.begin();
                while (it != clients.end())
                {
                    if (!it->isRunning())
                    {
                        it = clients.erase(it);
                    }
                    else
                    {
                        ++it;
                    }
                }
            }
        }
        catch (const std::exception& e)
        {
            std::cerr << "Exception in server: " << e.what() << std::endl;
            cleanup(-1);
        }
        catch (...)
        {
            std::cout << "Shutting down server..." << std::endl;
        }
        cleanup(0);
    }

    void cleanup(const int errorCode)
    {
        running = false;
        // Clean up all client connections
        for (auto& client : clients)
        {
            client.cleanup();
        }
        // Close server socket
        close(serverSocket);
        std::cout << "Server shutdown complete" << std::endl;
        exit(errorCode);
    }

    [[nodiscard]] bool isRunning() const { return running; }

private:
    std::string host = "0.0.0.0";
    int port = 5020;
    double emitRate; // tuples per second
    int serverSocket;
    sockaddr_in serverAddress;
    std::vector<ClientHandler> clients;
    bool running = true;
};

int main(const int argc, char* argv[])
{
    std::string host = "0.0.0.0";
    int port = 5020;
    double emitRate = 1.0; // default emit rate is 1 tuple per second

    for (int i = 1; i < argc; ++i)
    {
        if (std::strcmp(argv[i], "--host") == 0)
        {
            host = argv[++i];
        }
        else if (std::strcmp(argv[i], "-p") == 0 || std::strcmp(argv[i], "--port") == 0)
        {
            port = std::stoi(argv[++i]);
        }
        else if (std::strcmp(argv[i], "-r") == 0 || std::strcmp(argv[i], "--emit-rate") == 0)
        {
            emitRate = std::stod(argv[++i]);
        }
    }

    try
    {
        CounterServer server(host, port, emitRate);
        server.start();
    }
    catch (const std::exception& e)
    {
        std::cerr << "Exception in main: " << e.what() << std::endl;
    }
    catch (...)
    {
        std::cerr << "Unknown exception caught in main" << std::endl;
    }

    std::cout << "finished";
    return 0;
}
