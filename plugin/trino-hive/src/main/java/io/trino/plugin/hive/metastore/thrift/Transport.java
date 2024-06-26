/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.hive.metastore.thrift;

import com.google.common.net.HostAndPort;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import javax.net.ssl.SSLContext;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.Socket;
import java.util.Optional;

import static java.lang.String.format;
import static java.net.Proxy.Type.SOCKS;
import static java.util.Objects.requireNonNull;

public final class Transport
{
    public static TTransport create(
            HostAndPort address,
            Optional<SSLContext> sslContext,
            Optional<HostAndPort> socksProxy,
            int connectTimeoutMillis,
            int readTimeoutMillis,
            HiveMetastoreAuthentication authentication,
            Optional<String> delegationToken)
            throws TTransportException
    {
        requireNonNull(address, "address is null");
        try {
            TTransport rawTransport = createRaw(address, sslContext, socksProxy, connectTimeoutMillis, readTimeoutMillis);
            TTransport authenticatedTransport = authentication.authenticate(rawTransport, address.getHost(), delegationToken);
            if (!authenticatedTransport.isOpen()) {
                authenticatedTransport.open();
            }
            return new TTransportWrapper(authenticatedTransport, address);
        }
        catch (TTransportException e) {
            throw rewriteException(e, address);
        }
    }

    private Transport() {}

    private static TTransport createRaw(
            HostAndPort address,
            Optional<SSLContext> sslContext,
            Optional<HostAndPort> socksProxy,
            int connectTimeoutMillis,
            int readTimeoutMillis)
            throws TTransportException
    {
        Proxy proxy = socksProxy
                .map(socksAddress -> new Proxy(SOCKS, InetSocketAddress.createUnresolved(socksAddress.getHost(), socksAddress.getPort())))
                .orElse(Proxy.NO_PROXY);

        Socket socket = new Socket(proxy);
        try {
            socket.connect(new InetSocketAddress(address.getHost(), address.getPort()), connectTimeoutMillis);
            socket.setSoTimeout(readTimeoutMillis);

            if (sslContext.isPresent()) {
                // SSL will connect to the SOCKS address when present
                HostAndPort sslConnectAddress = socksProxy.orElse(address);

                socket = sslContext.get().getSocketFactory().createSocket(socket, sslConnectAddress.getHost(), sslConnectAddress.getPort(), true);
            }
            return new TSocket(socket);
        }
        catch (Throwable t) {
            // something went wrong, close the socket and rethrow
            try {
                socket.close();
            }
            catch (IOException e) {
                t.addSuppressed(e);
            }
            throw new TTransportException(t);
        }
    }

    private static TTransportException rewriteException(TTransportException e, HostAndPort address)
    {
        String message = e.getMessage() != null ? format("%s: %s", address, e.getMessage()) : address.toString();
        return new TTransportException(e.getType(), message, e);
    }

    private static class TTransportWrapper
            extends TFilterTransport
    {
        private final HostAndPort address;

        public TTransportWrapper(TTransport transport, HostAndPort address)
        {
            super(transport);
            this.address = requireNonNull(address, "address is null");
        }

        @Override
        public void open()
                throws TTransportException
        {
            try {
                transport.open();
            }
            catch (TTransportException e) {
                throw rewriteException(e, address);
            }
        }

        @Override
        public int readAll(byte[] bytes, int off, int len)
                throws TTransportException
        {
            try {
                return transport.readAll(bytes, off, len);
            }
            catch (TTransportException e) {
                throw rewriteException(e, address);
            }
        }

        @Override
        public int read(byte[] bytes, int off, int len)
                throws TTransportException
        {
            try {
                return transport.read(bytes, off, len);
            }
            catch (TTransportException e) {
                throw rewriteException(e, address);
            }
        }

        @Override
        public void write(byte[] bytes)
                throws TTransportException
        {
            try {
                transport.write(bytes);
            }
            catch (TTransportException e) {
                throw rewriteException(e, address);
            }
        }

        @Override
        public void write(byte[] bytes, int off, int len)
                throws TTransportException
        {
            try {
                transport.write(bytes, off, len);
            }
            catch (TTransportException e) {
                throw rewriteException(e, address);
            }
        }

        @Override
        public void flush()
                throws TTransportException
        {
            try {
                transport.flush();
            }
            catch (TTransportException e) {
                throw rewriteException(e, address);
            }
        }
    }
}
