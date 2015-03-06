
public class Cosine {
	Double cosine;
	Tweet tweet;
	public Cosine(Double cosine, Tweet tweet){
		this.cosine = cosine;
		this.tweet = tweet;
	}
	
	public Double getCosine(){
		return cosine;
	}
	
	 @Override
    public String toString() {
        return tweet.getValue().getText().toString() + " " + cosine.toString();
    }
}
