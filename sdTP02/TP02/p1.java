package TP02;

import TP02.ImagesWeb;
import java.util.concurrent.TimeoutException;
import com.rabbitmq.client.*;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;

//////////P1 Final  ///////// IMAGES + TEXTE

//compiler
//javac -cp amqp-client-5.1.2.jar:jsoup-1.11.2.jar TP02/p1.java TP02/ImagesWeb.java

//lancer
//java -cp .:amqp-client-5.1.2.jar:slf4j-api-1.7.25.jar:slf4j-simple-1.7.25.jar:jsoup-1.11.2.jar TP02/p1

public class p1 {

	private static final String siteWebImage = "https://www.uqar.ca/a-propos/langues/english/university/choosing-uqar";

	private static final String siteWebTexte = "https://www.uqar.ca/a-propos/langues/english/university/choosing-uqar";

	public static void main(String[] argv) throws java.io.IOException {

		String EXCHANGE_NAME = "exchange1";
		String NOM_FILE_DATTENTE_P1P3 = "fileImageP1";
		String cleDeLiaison_P1P3 = "imageP1.message"; 
		String nomUtilisateur = "guest"; 
		String motDePasse = "guest"; 
		int numeroPort = 5672; 
		String virtualHostName = "/"; 
		String hostName = "localhost";
		// String hostName = "192.168.183.129";

		boolean durable = true;
		boolean passive = false;
		boolean autoDelete = false; 
		boolean exclusive = false;

		// se connecter au broker RabbitMQ
		ConnectionFactory factory = new ConnectionFactory();

		// indiquer les parametres de la connexion
		factory.setUsername(nomUtilisateur);
		factory.setPassword(motDePasse);
		factory.setPort(numeroPort);
		factory.setVirtualHost(virtualHostName);
		factory.setHost(hostName);

		// creer une nouvelle connexion
		Connection connexion;
		try {
			connexion = factory.newConnection();

			// ouvrir un canal de communication avec le Broker pour l'envoi et la reception
			// de // messages
			Channel canalDeCommunication_P1P3 = connexion.createChannel();

			// Ordre exchangeDeclare --> exchange,type,durable,autodelete,passive(i),null
			// exchange,topic, true, false, false, null
			canalDeCommunication_P1P3.exchangeDeclare(EXCHANGE_NAME, "topic", durable, autoDelete, passive, null);
			canalDeCommunication_P1P3.queueDeclare(NOM_FILE_DATTENTE_P1P3, durable, exclusive, autoDelete, null);
			canalDeCommunication_P1P3.queueBind(NOM_FILE_DATTENTE_P1P3, EXCHANGE_NAME, cleDeLiaison_P1P3);
			canalDeCommunication_P1P3.basicQos(1);

			// https://examples.javacodegeeks.com/enterprise-java/html/download-images-from-a-website-using-jsoup/
			// Obtenir les element de type "img" (url de chaque image du site web)
			Document doc = Jsoup.connect(siteWebImage).get();
			Elements imgs = doc.getElementsByTag("img");

			for (Element el : imgs) {
				String src = el.absUrl("src");
				System.out.println("src attribute is : " + src);

				// message de type byte[] a envoyer a p3
				byte[] imageAenvoyer = ImagesWeb.getImages(src);

				// recuperer le nom complet du fichier
				String nomCompletDuFichier = ImagesWeb.getImagesName(src);
				System.out.println("nom  au complet du fichier : " + nomCompletDuFichier);

				// Extraire juste le nom du fichier
				String justeNom = ImagesWeb.extractName(nomCompletDuFichier);
				System.out.println("Extraire juste nom du fichier : " + justeNom);

				// Extrair le type du fichier
				String typeFidchier = ImagesWeb.extractExtension(nomCompletDuFichier);
				System.out.println("Type du fichier : " + typeFidchier);

				// creer le hashmap contenant le nom et type du fichier
				Map<String, Object> headers = new HashMap<String, Object>();
				headers.put("nom", justeNom);
				headers.put("type", typeFidchier);

				// Ne pas envoyer que des images jpg ou png ou gif
				if ((typeFidchier.equals("jpg")) || (typeFidchier.equals("png")) || (typeFidchier.equals("gif"))) {
					// Envoyer l'image a P3
					canalDeCommunication_P1P3.basicPublish("", NOM_FILE_DATTENTE_P1P3,
							new AMQP.BasicProperties.Builder().headers(headers).build(), imageAenvoyer);
					System.out.println("P1 a envoye une image a P3!!");
				} else {
					System.out.println("          Attention : TYPE D'IMAGE PAS DESIRE!!");
				}
			} // end for (Element el : imgs)

			// fermer le canal
			canalDeCommunication_P1P3.close();

			// fermer la connexion
			connexion.close();

		} catch (TimeoutException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		//////////////////////// Section Texte ////////////////////////////////////
		// se connecter au broker RabbitMQ

		String NOM_FILE_DATTENTE_P1P2 = "fileTexteP1";
		String cleDeLiaison_P1P2 = "texteP1.message"; // cle de liaison/routage du message

		ConnectionFactory factory2 = new ConnectionFactory();

		// indiquer les parametres de la connexion
		factory2.setUsername(nomUtilisateur);
		factory2.setPassword(motDePasse);
		factory2.setPort(numeroPort);
		factory2.setVirtualHost(virtualHostName);
		factory2.setHost(hostName);

		// creer une nouvelle connexion
		Connection connexion2;
		try {
			connexion2 = factory2.newConnection();

			// ouvrir un canal de communication avec le Broker pour l'envoi et la reception
			// de // messages
			Channel canalDeCommunication_P1P2 = connexion2.createChannel();

			// Ordre exchangeDeclare --> exchange,type,durable,autodelete,passive(i),null
			// exchange,topic, true, false, false, null
			canalDeCommunication_P1P2.exchangeDeclare(EXCHANGE_NAME, "topic", durable, autoDelete, passive, null);

			canalDeCommunication_P1P2.queueDeclare(NOM_FILE_DATTENTE_P1P2, durable, exclusive, autoDelete, null);
			canalDeCommunication_P1P2.queueBind(NOM_FILE_DATTENTE_P1P2, EXCHANGE_NAME, cleDeLiaison_P1P2);
			canalDeCommunication_P1P2.basicQos(1);
			
			String html = "";
			// https://examples.javacodegeeks.com/core-java/net/url/read-text-from-url/
			try {

				URL url = new URL(siteWebTexte);
				// read text returned by server
				BufferedReader in = new BufferedReader(new InputStreamReader(url.openStream()));

				String line;
				while ((line = in.readLine()) != null) {
					html += line;
					// System.out.println(line);
				}
				in.close();

				// System.out.println(html);
			} catch (MalformedURLException e) {
				System.out.println("Malformed URL: " + e.getMessage());
			} catch (IOException e) {
				System.out.println("I/O Error: " + e.getMessage());
			}

			// https://www.tutorialspoint.com/jsoup/jsoup_extract_text.htm
			Document document = Jsoup.parse(html);

			// Extraire le texte qui se trouve entre les balises <p>
			Elements link = document.select("p");
			String texteTraduit = link.text().toString();
			String[] temp;
			String delimeter = "\\.";
			temp = texteTraduit.split(delimeter);

			System.out.println("Nombre des paragraphes : " + temp.length);

			for (int i = 0; i < temp.length; i++) {
				String phraseAEnvoyer = temp[i];
				System.out.println("Message # " + i + " :" + phraseAEnvoyer);
				// Envoyer chaque paragraphe a P2
				canalDeCommunication_P1P2.basicPublish("", NOM_FILE_DATTENTE_P1P2, null, phraseAEnvoyer.getBytes());

			}

			// fermer le canal
			canalDeCommunication_P1P2.close();

			// fermer la connexion
			connexion2.close();

		} catch (TimeoutException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}// end public static void main
}// end public class p1
